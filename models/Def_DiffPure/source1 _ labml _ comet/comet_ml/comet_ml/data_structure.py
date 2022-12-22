# -*- coding: utf-8 -*-
# *******************************************************
#   ____                     _               _
#  / ___|___  _ __ ___   ___| |_   _ __ ___ | |
# | |   / _ \| '_ ` _ \ / _ \ __| | '_ ` _ \| |
# | |__| (_) | | | | | |  __/ |_ _| | | | | | |
#  \____\___/|_| |_| |_|\___|\__(_)_| |_| |_|_|
#
#  Sign up for free at http://www.comet.ml
#  Copyright (C) 2015-2021 Comet ML INC
#  This file can not be copied and/or distributed without the express
#  permission of Comet ML Inc.
# *******************************************************

from __future__ import print_function

import json
import logging
import math
import os

from ._typing import List, Optional
from .convert_utils import HAS_NUMPY, fast_flatten, numpy
from .utils import log_once_at_level

LOGGER = logging.getLogger(__name__)


class Embedding(object):
    """
    Data structure for holding embedding template info.
    """

    def __init__(
        self,
        vector_url,
        vector_shape,
        metadata_url,
        sprite_url=None,
        image_size=None,
        title="Comet Embedding",
    ):
        """
        The URL's and sizes for all of the Embedding'
        resources.
        """
        self.vector_url = vector_url
        self.vector_shape = vector_shape
        self.metadata_url = metadata_url
        self.sprite_url = sprite_url
        self.image_size = image_size
        self.title = title

    def to_json(self):
        """
        Return a JSON representation of the embedding
        """
        template = {
            "tensorName": self.title,
            "tensorShape": list(self.vector_shape),
            "tensorPath": self.vector_url,
            "metadataPath": self.metadata_url,
        }
        if self.sprite_url is not None:
            template["sprite"] = {
                "imagePath": self.sprite_url,
                "singleImageDim": list(self.image_size),
            }
        return template


class Histogram(object):
    """
    Data structure for holding a counts of values. Creates an
    exponentially-distributed set of bins.

    See also [`Experiment.log_histogram`](#experimentlog_histogram)
    """

    def __init__(self, start=1e-12, stop=1e20, step=1.1, offset=0):
        """
        Initialize the values of bins and data structures.

        Args:
            start: float (optional, deprecated), value of start range. Default 1e-12
            stop: float (optional, deprecated), value of stop range. Default 1e20
            step: float (optional, deprecated), value of step. Creates an
                exponentially-distributed set of bins. Must be greater
                than 1.0. Default 1.1
            offset: float (optional), center of distribution. Default is zero.
        """
        if start != 1e-12 or stop != 1e20 or step != 1.1:
            log_once_at_level(
                logging.WARNING,
                "Histogram will deprecate changing start, stop, or step values in a future version",
            )

        if step <= 1.0:
            raise ValueError("Histogram step must be greater than 1.0")

        if start <= 0:
            raise ValueError("Histogram start must be greater than 0.0")

        if stop <= start:
            raise ValueError("Histogram stop must be greater than start")

        self.start = start
        self.stop = stop
        self.step = step
        self.offset = offset
        self.values = tuple(self.create_bin_values())
        self.clear()

    def __sub__(self, other):
        if (
            isinstance(other, Histogram)
            and (self.start == other.start)
            and (self.stop == other.stop)
            and (self.step == other.step)
            and (self.offset == other.offset)
        ):

            histogram = Histogram(self.start, self.stop, self.step, self.offset)
            histogram.counts = [
                abs(c2 - c1) for c1, c2 in zip(self.counts, other.counts)
            ]
            return histogram
        else:
            raise TypeError("Can't subtract Histograms of different regions")

    def clear(self):
        """
        Clear the counts, initializes back to zeros.
        """
        self.counts = [0] * len(self.values)
        if HAS_NUMPY:
            self.counts = numpy.array(self.counts)

    def get_bin_index(self, value):
        """
        Given a value, return the bin index where:

            values[index] <= value < values[index + 1]
        """
        midpoint = len(self.counts) // 2
        if value >= self.stop:
            return len(self.values) - 2  # slight asymmetry
        elif value <= -self.stop:
            return 0
        base = self.step
        if value - self.offset == 0:
            return midpoint
        elif value - self.offset < 0:
            return (
                midpoint
                - 1
                - int(math.ceil(math.log(abs(value - self.offset) / self.start, base)))
            )
        else:
            return (
                int(math.ceil(math.log((value - self.offset) / self.start, base)))
                + midpoint
            )

    def _add_via_python(self, values, counts=None, max_skip_count=None):
        # type: (List, Optional[List], int) -> None
        """
        Values is a Python list. Only used
        when numpy is not available.
        """
        max_skip_count = max_skip_count if max_skip_count is not None else 10

        # Sort for speed of inserts
        if counts is None:
            values.sort()

        # Find initial bin:
        bucket = self.get_bin_index(values[0])

        # Avoid attributes lookup at every loop
        self_values = self.values
        self_counts = self.counts

        for i, value in enumerate(values):
            skip_count = 0
            while not (
                (bucket == 0 and value <= self_values[1])
                or (self_values[bucket] <= value < self_values[bucket + 1])
            ):
                skip_count += 1
                if skip_count > max_skip_count:
                    # if too many skips
                    bucket = self.get_bin_index(value)
                    break
                else:
                    bucket += 1

            if counts is not None:
                self_counts[bucket] += int(counts[i])
            else:
                self_counts[bucket] += 1

    def _add_via_numpy(self, values, counts=None):
        # type: (numpy.ndarray, Optional[numpy.ndarray]) -> None
        """
        Assumes numpy values (and counts).
        """
        midpoint = len(self.counts) // 2
        base = numpy.log(self.step)

        # Positive values (values > offset)

        # The filter is an array of boolean same size as values
        positive_values_filter = numpy.bitwise_and(
            values > self.offset, values < self.stop
        )

        # This is a numpy array with only positive values and we center them
        # around the offset (default 0 so no-op)
        positive_values = values[positive_values_filter] - self.offset

        # Then we transform all values to the suite index, and we add the
        # midpoint as we are storing positives (values > offset) at the end of
        # the self.values array
        positive_indices = (
            numpy.ceil(numpy.log(positive_values / self.start) / base).astype(int)
            + midpoint
        )

        # Negative values (values > offset)

        # The filter is an array of boolean same size as values
        negative_values_filter = numpy.bitwise_and(
            values < self.offset, values > -self.stop
        )

        # This is a numpy array with only negatives values and we center them
        # around the offset (default 0 so no-op)
        negative_values = values[negative_values_filter] - self.offset

        # Then we transform all values to the suite index, and substract the
        # index from the midpoint as we are storing positives (values > offset)
        # at the end of the self.values array
        negative_indices = (
            midpoint
            - 1
            - numpy.ceil(
                numpy.log(numpy.absolute(negative_values) / self.start) / base
            ).astype(int)
        )

        # Zero values (values == offset)

        # The filter is an array of boolean same size as values
        zero_values_filter = values == self.offset

        # This is a numpy array with only zero values
        zero_values = values[zero_values_filter]

        if counts is not None:
            match_count = 0
            for i, match in enumerate(positive_values_filter):
                if match:
                    self.counts[positive_indices[match_count]] += counts[i]
                    match_count += 1
            match_count = 0
            for i, match in enumerate(negative_values_filter):
                if match:
                    self.counts[negative_indices[match_count]] += counts[i]
                    match_count += 1
            for i, match in enumerate(zero_values_filter):
                if match:
                    self.counts[midpoint] += counts[i]
        else:
            numpy.add.at(self.counts, positive_indices, 1)
            numpy.add.at(self.counts, negative_indices, 1)
            self.counts[midpoint] += len(zero_values)

    def add(self, values, counts=None, max_skip_count=None):
        """
        Add the value(s) to the count bins.

        Args:
            values: a list, tuple, or array of values (any shape)
                to count
            counts: a list of counts for each value in values. Triggers
                special mode for conversion from Tensorboard
                saved format. Assumes values are in order (min to max).
            max_skip_count: int, (optional) maximum number of missed
                bins that triggers get_bin_index() (only used
                when numpy is not available).

         Counting values in bins can be expensive, so this method uses
        numpy where possible.
        """
        try:
            values = [float(values)]
        except Exception:
            pass

        # Returns numpy array, if possible:
        values = fast_flatten(values)

        if len(values) == 0:
            return

        if HAS_NUMPY:
            if counts is not None:
                counts = numpy.array(counts)
            self._add_via_numpy(values, counts)
        else:
            self._add_via_python(values, counts, max_skip_count)

    def counts_compressed(self):
        """
        Convert list of counts to list of [(index, count), ...].
        """
        return [[i, int(count)] for (i, count) in enumerate(self.counts) if count > 0]

    @classmethod
    def from_json(cls, data, verbose=True):
        """
        Given histogram JSON data, returns either a Histogram object (in
        the case of a 2D histogram) or a list of Histogram objects (in
        the case of a 3D histogram).

        Args:
            data: histogram filename or JSON data
            verbose: optional, bool, if True, display the Histogram
                after conversion

        Returns: a Histogram, or list of Histograms.

        Example:

        ```python
        In[1]: histogram = Histogram.from_json("histogram.json")

        Histogram 3D:
        Step: 0
        Histogram
        =========
        Range Start      Range End          Count           Bins
        -----------------------------------------------------------
            -0.1239        -0.1003        63.7810      [506-508]
            -0.1003        -0.0766      1006.1836      [508-511]
            -0.0766        -0.0530      1824.0884      [511-514]
            -0.0530        -0.0293     11803.4008      [514-521]
            -0.0293        -0.0056     13118.4797      [521-538]
            -0.0056         0.0180     13059.5624     [538-1023]
             0.0180         0.0417     13133.6479    [1023-1032]
             0.0417         0.0654      5865.2828    [1032-1037]
             0.0654         0.0890      1616.2949    [1037-1040]
             0.0890         0.1127       275.2784    [1040-1042]
             0.1127         0.1363         0.0000    [1042-1044]
        -----------------------------------------------------------
        Total:     61766.0000
        Out[1]:
        <comet_ml.utils.Histogram at 0x7fac8f3819b0>
        ```
        """
        if isinstance(data, str):
            filename = os.path.expanduser(data)
            if os.path.isfile(filename):
                with open(filename) as fp:
                    histogram_json = json.load(fp)
                    return Histogram.from_json(histogram_json, verbose=verbose)
            else:
                LOGGER.error("Histogram.from_json: no such file %r", filename)
                return
        elif "histograms" in data:
            retval = []
            if verbose:
                print("Histogram 3D:")
            for datum in data["histograms"]:
                histogram_json = datum["histogram"]
                if verbose:
                    print("Step:", datum["step"])
                histogram = Histogram.from_json(histogram_json, verbose=verbose)
                retval.append(histogram)
            return retval
        else:
            histogram = Histogram(
                data["start"], data["stop"], data["step"], data["offset"]
            )
            for (i, count) in data["index_values"]:
                histogram.counts[i] = count
            if verbose:
                histogram.display()
            return histogram

    def to_json(self):
        """
        Return histogram as JSON-like dict.
        """
        return {
            "version": 2,
            "index_values": self.counts_compressed(),
            "values": None,
            "offset": self.offset,
            "start": self.start,
            "stop": self.stop,
            "step": self.step,
        }

    def is_empty(self):
        # type: () -> bool
        """Check if the Histogram is empty. Return True if empty, False otherwise"""
        # If the Histogram contains at least one value, at least one element of
        # self.counts will be not null
        return not any(self.counts)

    def create_bin_values(self):
        """
        Create exponentially distributed bin values
        [-inf, ..., offset - start, offset, offset + start, ..., inf)
        """
        values = [-float("inf"), self.offset, float("inf")]
        value = self.start
        while self.offset + value <= self.stop:
            values.insert(1, self.offset - value)
            values.insert(-1, self.offset + value)
            value *= self.step
        return values

    def get_count(self, min_value, max_value):
        """
        Get the count (can be partial of bin count) of a range.
        """
        index = self.get_bin_index(min_value)
        current_start_value = self.values[index]
        current_stop_value = self.values[index + 1]
        count = 0
        # Add total in this area:
        count += self.counts[index]
        if current_start_value != -float("inf"):
            # Remove proportion before min_value:
            current_total_range = current_stop_value - current_start_value

            if max_value < current_stop_value:
                percent = (current_stop_value - max_value) / current_total_range
                count -= self.counts[index] * percent
            else:
                percent = (min_value - current_start_value) / current_total_range
                count -= self.counts[index] * percent

        if max_value < current_stop_value:
            # stop is inside this area too, so remove after max
            return count

        # max_value is beyond this area, so loop until last area:
        index += 1
        while max_value > self.values[index + 1]:
            # add the whole count
            count += self.counts[index]
            index += 1
        # finally, add the proportion in last area before max_value:
        current_start_value = self.values[index]
        current_stop_value = self.values[index + 1]
        if current_stop_value != float("inf"):
            current_total_range = current_stop_value - current_start_value
            percent = (max_value - current_start_value) / current_total_range
            count += self.counts[index] * percent
        else:
            count += self.counts[index]
        return count

    def get_counts(self, min_value, max_value, span_value):
        """
        Get the counts between min_value and max_value in
        uniform span_value-sized bins.
        """
        counts = []

        if max_value == min_value:
            max_value = min_value * 1.1 + 1
            min_value = min_value / 1.1 - 1

        bucketPos = 0
        binLeft = min_value

        while binLeft < max_value:
            binRight = binLeft + span_value
            count = 0.0
            # Don't include last as bucketLeft, which is infinity:
            while bucketPos < len(self.values) - 1:
                bucketLeft = self.values[bucketPos]
                bucketRight = min(max_value, self.values[bucketPos + 1])
                intersect = min(bucketRight, binRight) - max(bucketLeft, binLeft)

                if intersect > 0:
                    if bucketLeft == -float("inf"):
                        count += self.counts[bucketPos]
                    else:
                        count += (intersect / (bucketRight - bucketLeft)) * self.counts[
                            bucketPos
                        ]

                if bucketRight > binRight:
                    break

                bucketPos += 1

            counts.append(count)
            binLeft += span_value

        return counts

    def display(
        self, start=None, stop=None, step=None, format="%14.4f", show_empty=False
    ):
        """
        Show counts between start and stop by step increments.

        Args:
            start: optional, float, start of range to display
            stop: optional, float, end of range to display
            step: optional, float, amount to increment each range
            format: str (optional), format of numbers
            show_empty: bool (optional), if True, show all
                entries in range

        Example:

        ```
        >>> from comet_ml.utils import Histogram
        >>> import random
        >>> history = Histogram()
        >>> values = [random.random() for x in range(10000)]
        >>> history.add(values)
        >>> history.display()

        Histogram
        =========
           Range Start      Range End          Count           Bins
        -----------------------------------------------------------
               -0.0000         0.1000       983.4069     [774-1041]
                0.1000         0.2000       975.5574    [1041-1049]
                0.2000         0.3000      1028.8666    [1049-1053]
                0.3000         0.4000       996.2112    [1053-1056]
                0.4000         0.5000       979.5836    [1056-1058]
                0.5000         0.6000      1010.4522    [1058-1060]
                0.6000         0.7000       986.1284    [1060-1062]
                0.7000         0.8000      1006.5811    [1062-1063]
                0.8000         0.9000      1007.7881    [1063-1064]
                0.9000         1.0000      1025.4245    [1064-1065]
        -----------------------------------------------------------
        Total:     10000.0000
        """
        collection = self.collect(start, stop, step)
        print("Histogram")
        print("=========")
        size = len(format % 0)
        sformat = "%" + str(size) + "s"
        columns = ["Range Start", "Range End", "Count", "Bins"]
        formats = [sformat % s for s in columns]
        print(*formats)
        print("-" * (size * 4 + 3))
        total = 0.0
        for row in collection:
            count = row["count"]
            total += count
            if show_empty or count > 0:
                print(
                    format % row["value_start"],
                    format % row["value_stop"],
                    format % count,
                    (
                        sformat
                        % ("[%s-%s]" % (row["bin_index_start"], row["bin_index_stop"]))
                    ),
                )
        print("-" * (size * 4 + 3))
        print(("Total: " + format) % total)

    def collect(self, start=None, stop=None, step=None):
        """
        Collect the counts for the given range and step.

        Args:
            start: optional, float, start of range to display
            stop: optional, float, end of range to display
            step: optional, float, amount to increment each range

        Returns a list of dicts containing details on each
        virtual bin.
        """
        counts_compressed = self.counts_compressed()
        if start is None:
            if len(counts_compressed) > 0:
                start = self.values[counts_compressed[0][0]]
            else:
                start = -1.0
        if stop is None:
            if len(counts_compressed) > 1:
                stop = self.values[counts_compressed[-1][0]]
            else:
                stop = 1.0
        if step is None:
            step = (stop - start) / 10.0

        counts = self.get_counts(start, stop + step, step)
        current = start
        bins = []
        next_one = current + step
        i = 0
        while next_one <= stop + step and i < len(counts):
            start_bin = self.get_bin_index(current)
            stop_bin = self.get_bin_index(next_one)
            current_bin = {
                "value_start": current,
                "value_stop": next_one,
                "bin_index_start": start_bin,
                "bin_index_stop": stop_bin,
                "count": counts[i],
            }
            bins.append(current_bin)
            current = next_one
            next_one = current + step
            i += 1
        return bins
