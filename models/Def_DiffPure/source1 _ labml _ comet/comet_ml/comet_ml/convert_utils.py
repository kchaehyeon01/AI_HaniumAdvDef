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

import inspect
import io
import json
import logging
import math
import numbers

import six

from ._typing import (
    IO,
    Any,
    Callable,
    Dict,
    List,
    Number,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)
from .compat_utils import Mapping, StringIO
from .logging_messages import (
    CONVERT_DATAFRAME_INVALID_FORMAT,
    CONVERT_TABLE_INVALID_FORMAT,
    DATAFRAME_CONVERSION_ERROR,
    INVALID_BOUNDING_BOX_3D,
    INVALID_BOUNDING_BOXES_3D,
    INVALID_CLOUD_POINTS_3D,
    INVALID_SINGLE_CLOUD_POINT_3D,
    INVALID_SINGLE_CLOUD_POINT_3D_LENGTH,
    METRIC_ARRAY_WARNING,
)
from .schemas import get_3d_boxes_validator
from .utils import is_list_like, log_once_at_level

try:
    import pathlib
except ImportError:
    try:
        import pathlib2 as pathlib
    except ImportError:
        pathlib = None

LOGGER = logging.getLogger(__name__)

INFINITY = float("inf")

try:
    import numpy

    HAS_NUMPY = True
except ImportError:
    LOGGER.warning("numpy not installed; some functionality will be unavailable")
    HAS_NUMPY = False
    numpy = None


def convert_to_bytes(item):
    """
    Convert the item into bytes. The conversion may not
    be reversable.
    """
    if hasattr(item, "tobytes"):
        # Numpy arrays, etc.
        return item.tobytes()
    else:
        str_item = str(item)
        return str_item.encode(encoding="utf-8", errors="xmlcharrefreplace")


def convert_tensor_to_numpy(tensor):
    """
    Convert from various forms of pytorch tensors
    to numpy arrays.

    Note: torch tensors can have both "detach" and "numpy"
    methods, but numpy() alone will fail if tensor.requires_grad
    is True.
    """
    if hasattr(tensor, "detach"):  # pytorch tensor with attached gradient
        tensor = tensor.detach()

    if hasattr(tensor, "numpy"):  # pytorch tensor
        tensor = tensor.numpy()

    return tensor


def convert_to_scalar(user_data, dtype=None):
    # type: (Any, Optional[Type]) -> Any
    """
    Try to ensure that the given user_data is converted back to a
    Python scalar, and of proper type (if given).
    """
    # Fast-path for types and class, we currently does not apply any conversion
    # to classes
    if inspect.isclass(user_data):

        if dtype and not isinstance(user_data, dtype):
            raise TypeError("%r is not of type %r" % (user_data, dtype))

        return user_data

    # First try to convert tensorflow tensor to numpy objects
    try:
        if hasattr(user_data, "numpy"):
            user_data = user_data.numpy()
    except Exception:
        LOGGER.debug(
            "Failed to convert tensorflow tensor %r to numpy object",
            user_data,
            exc_info=True,
        )

    # Then try to convert numpy object to a Python scalar
    try:
        if hasattr(user_data, "item") and callable(user_data.item):
            user_data = user_data.item()
    except Exception:
        LOGGER.debug(
            "Failed to convert object %r to Python scalar",
            user_data,
            exc_info=True,
        )

    if dtype is not None and not isinstance(user_data, dtype):
        raise TypeError("%r is not of type %r" % (user_data, dtype))

    return user_data


def convert_to_list(items, dtype=None):
    # type: (Any, Any) -> List[Any]
    """
    Take an unknown item and convert to a list of scalars
    and ensure type is dtype, if given.
    """
    # First, convert it to numpy if possible:
    if hasattr(items, "numpy"):  # pytorch tensor
        items = convert_tensor_to_numpy(items)
    elif hasattr(items, "eval"):  # tensorflow tensor
        items = items.eval()

    # Next, handle numpy array:
    if hasattr(items, "tolist"):
        if len(items.shape) != 1:
            raise ValueError("list should be one dimensional")
        result = items.tolist()  # type: List[Any]
        return result
    else:
        # assume it is something with numbers in it:
        return [convert_to_scalar(item, dtype=dtype) for item in items]


def validate_single_3d_point(single_3d_point):
    # type: (Any) -> Optional[List[Number]]

    try:
        convert_point_list = convert_to_list(single_3d_point)
    except Exception:
        LOGGER.warning(
            INVALID_SINGLE_CLOUD_POINT_3D,
            single_3d_point,
            exc_info=True,
            extra={"show_traceback": True},
        )
        return None

    if len(convert_point_list) < 3:
        LOGGER.warning(INVALID_SINGLE_CLOUD_POINT_3D_LENGTH, single_3d_point)
        return None

    return convert_point_list


def validate_and_convert_3d_points(points):
    # type: (Any) -> List[List[Number]]
    if points is None:
        return []

    final_points = []

    try:
        for point in points:
            convert_point = validate_single_3d_point(point)

            if convert_point is not None:
                final_points.append(convert_point)
    except Exception:
        LOGGER.warning(
            INVALID_CLOUD_POINTS_3D,
            points,
            exc_info=True,
            extra={"show_traceback": True},
        )
        return []

    return final_points


def validate_single_3d_box(validator, box):
    # type: (Any, Any) -> Optional[Dict[str, Any]]
    try:
        # First reconstruct box with converted types
        converted_box = {
            "position": convert_to_list(box.get("position", [])),
            "size": {
                "height": convert_to_scalar(box.get("size", {}).get("height", None)),
                "width": convert_to_scalar(box.get("size", {}).get("width", None)),
                "depth": convert_to_scalar(box.get("size", {}).get("depth", None)),
            },
            "label": box.get("label", None),
        }

        # Optional fields
        box_rotation = box.get("rotation", None)
        if box_rotation is not None:
            converted_box.update(
                {
                    "rotation": {
                        "alpha": convert_to_scalar(box_rotation.get("alpha", None)),
                        "beta": convert_to_scalar(box_rotation.get("beta", None)),
                        "gamma": convert_to_scalar(box_rotation.get("gamma", None)),
                    }
                }
            )

        box_color = box.get("color", None)
        if box_color is not None:
            converted_box["color"] = convert_to_list(box_color)

        box_probability = box.get("probability", None)
        if box_probability is not None:
            converted_box["probability"] = convert_to_scalar(box_probability)

        box_class = box.get("class", None)
        if box_class is not None:
            converted_box["class"] = box_class

        validator.validate(converted_box)
        return converted_box
    except Exception:
        LOGGER.warning(
            INVALID_BOUNDING_BOX_3D,
            box,
            exc_info=True,
            extra={"show_traceback": True},
        )

        return None


def validate_and_convert_3d_boxes(boxes):
    # type: (Any) -> List[Dict[str, Any]]
    if boxes is None:
        return []

    validator = get_3d_boxes_validator()
    final_boxes = []

    try:
        for box in boxes:
            converted_box = validate_single_3d_box(validator, box)

            if converted_box is not None:
                final_boxes.append(converted_box)
    except Exception:
        LOGGER.warning(
            INVALID_BOUNDING_BOXES_3D,
            boxes,
            exc_info=True,
            extra={"show_traceback": True},
        )

    return final_boxes


def convert_pathlib_path(user_input):
    # type: (Any) -> Any
    if pathlib is not None and isinstance(user_input, pathlib.Path):
        return str(user_input)

    return user_input


def fix_special_floats(value, _inf=INFINITY, _neginf=-INFINITY):
    """Fix out of bounds floats (like infinity and -infinity) and Not A
    Number.
    Returns either a fixed value that could be JSON encoded or the original
    value.
    """

    try:
        value = convert_tensor_to_numpy(value)

        # Check if the value is Nan, equivalent of math.isnan
        if math.isnan(value):
            return "NaN"

        elif value == _inf:
            return "Infinity"

        elif value == _neginf:
            return "-Infinity"

    except Exception:
        # Value cannot be compared
        return value

    return value


def image_data_to_file_like_object(
    image_data,
    file_name,
    image_format,
    image_scale,
    image_shape,
    image_colormap,
    image_minmax,
    image_channels,
):
    # type: (Union[IO[bytes], Any], Optional[str], str, float, Optional[Sequence[int]], Optional[str], Optional[Sequence[float]], str) -> Union[IO[bytes], None, Any]
    """
    Ensure that the given image_data is converted to a file_like_object ready
    to be uploaded
    """
    try:
        import PIL.Image
    except ImportError:
        PIL = None

    ## Conversion from standard objects to image
    ## Allow file-like objects, numpy arrays, etc.
    if hasattr(image_data, "numpy"):  # pytorch tensor
        array = convert_tensor_to_numpy(image_data)
        fp = array_to_image_fp(
            array,
            image_format,
            image_scale,
            image_shape,
            image_colormap,
            image_minmax,
            image_channels,
        )

        return fp
    elif hasattr(image_data, "eval"):  # tensorflow tensor
        array = image_data.eval()
        fp = array_to_image_fp(
            array,
            image_format,
            image_scale,
            image_shape,
            image_colormap,
            image_minmax,
            image_channels,
        )

        return fp
    elif PIL is not None and isinstance(image_data, PIL.Image.Image):  # PIL.Image
        ## filename tells us what format to use:
        if file_name is not None and "." in file_name:
            _, image_format = file_name.rsplit(".", 1)
        fp = image_to_fp(image_data, image_format)

        return fp
    elif image_data.__class__.__name__ == "ndarray":  # numpy array
        fp = array_to_image_fp(
            image_data,
            image_format,
            image_scale,
            image_shape,
            image_colormap,
            image_minmax,
            image_channels,
        )

        return fp
    elif hasattr(image_data, "read"):  # file-like object
        return image_data
    elif isinstance(image_data, (tuple, list)):  # list or tuples
        if not HAS_NUMPY:
            LOGGER.error("The Python library numpy is required for this operation")
            return None
        array = numpy.array(image_data)
        fp = array_to_image_fp(
            array,
            image_format,
            image_scale,
            image_shape,
            image_colormap,
            image_minmax,
            image_channels,
        )
        return fp
    else:
        LOGGER.error("invalid image file_type: %s", type(image_data))
        if PIL is None:
            LOGGER.error("Consider installing the Python Image Library, PIL")
        return None


def array_to_image_fp(
    array,
    image_format,
    image_scale,
    image_shape,
    image_colormap,
    image_minmax,
    image_channels,
):
    # type: (Any, str, float, Optional[Sequence[int]], Optional[str], Optional[Sequence[float]], str) -> Optional[IO[bytes]]
    """
    Convert a numpy array to an in-memory image
    file pointer.
    """
    image = array_to_image(
        array, image_scale, image_shape, image_colormap, image_minmax, image_channels
    )
    if not image:
        return None
    return image_to_fp(image, image_format)


def array_to_image(
    array,
    image_scale=1.0,
    image_shape=None,
    image_colormap=None,
    image_minmax=None,
    image_channels=None,
    mode=None,
):
    # type: (Any, float, Optional[Sequence[int]], Optional[str], Optional[Sequence[float]], Optional[str], Optional[str]) -> Optional[Any]
    """
    Convert a numpy array to an in-memory image.
    """
    try:
        import numpy
        import PIL.Image
        from matplotlib import cm
    except ImportError:
        LOGGER.error(
            "The Python libraries PIL, numpy, and matplotlib are required for converting a numpy array into an image"
        )
        return None

    array = numpy.array(array)

    ## Handle image transformations here
    ## End up with a 0-255 PIL Image
    if image_minmax is not None:
        minmax = image_minmax
    else:  # auto minmax
        flatten_array = flatten(array)
        min_array = min(flatten_array)
        max_array = max(flatten_array)
        if min_array == max_array:
            min_array = min_array - 0.5
            max_array = max_array + 0.5
        min_array = math.floor(min_array)
        max_array = math.ceil(max_array)
        minmax = [min_array, max_array]

    ## if a shape is given, try to reshape it:
    if image_shape is not None:
        try:
            ## array shape is opposite of image size(width, height)
            if len(image_shape) == 2:
                array = array.reshape(image_shape[1], image_shape[0])
            elif len(image_shape) == 3:
                array = array.reshape(image_shape[1], image_shape[0], image_shape[2])
            else:
                raise Exception(
                    "invalid image_shape: %s; should be 2D or 3D" % image_shape
                )
        except Exception:
            LOGGER.info("WARNING: invalid image_shape; ignored", exc_info=True)

    if image_channels == "first" and len(array.shape) == 3:
        array = numpy.moveaxis(array, 0, -1)
    ## If 3D, but last array is flat, make it 2D:
    if len(array.shape) == 3:
        if array.shape[-1] == 1:
            array = array.reshape((array.shape[0], array.shape[1]))
        elif array.shape[0] == 1:
            array = array.reshape((array.shape[1], array.shape[2]))
    elif len(array.shape) == 1:
        ## if 1D, make it 2D:
        array = numpy.array([array])

    ### Ok, now let's colorize and scale
    if image_colormap is not None:
        ## Need to be in range (0,1) for colormapping:
        array = rescale_array(array, minmax, (0, 1), "float")
        try:
            cm_hot = cm.get_cmap(image_colormap)
            array = cm_hot(array)
        except Exception:
            LOGGER.info("WARNING: invalid image_colormap; ignored", exc_info=True)
        ## rescale again:
        array = rescale_array(array, (0, 1), (0, 255), "uint8")
        ## Convert to RGBA:
        image = PIL.Image.fromarray(array, "RGBA")
    else:
        ## Rescale (0, 255)
        array = rescale_array(array, minmax, (0, 255), "uint8")
        image = PIL.Image.fromarray(array)

    if image_scale != 1.0:
        image = image.resize(
            (int(image.size[0] * image_scale), int(image.size[1] * image_scale))
        )

    ## Put in a standard mode:
    if mode:
        image = image.convert(mode)
    elif image.mode not in ["RGB", "RGBA"]:
        image = image.convert("RGB")
    return image


def dataset_to_sprite_image(
    matrix,
    size,
    preprocess_function=None,
    transparent_color=None,
    background_color_function=None,
):
    # type: (Any, Sequence[int], Optional[Callable], Optional[Tuple[int, int, int]], Optional[Callable]) -> Any
    """
    Convert a dataset (array of arrays) into a giant image of
    images (a sprite sheet).

    Args:
        matrix: array of vectors or Images
        size: (width, height) of each thumbnail image
        preprocess_function: function to preprocess image values
        transparent_color: color to mark as transparent
        background_color_function: function that takes index, returns a color

    Returns: image
    """
    try:
        from PIL import Image
    except ImportError:
        LOGGER.error("The Python library PIL is required for this operation")
        return None

    length = len(matrix)
    sprite_size = math.ceil(math.sqrt(length))

    sprite_image = Image.new(
        mode="RGBA",
        size=(int(sprite_size * size[0]), int(sprite_size * size[1])),
        color=(0, 0, 0, 0),
    )
    if preprocess_function is not None:
        matrix = preprocess_function(matrix)
    for i, array in enumerate(matrix):
        if isinstance(array, Image.Image):
            image = array
        else:
            image = array_to_image(
                array,
                image_scale=1.0,
                image_shape=size,
                image_colormap=None,
                image_minmax=(0, 255),
                image_channels="last",
                mode="RGBA",
            )

            if image is None:
                return None

        if transparent_color is not None:
            image = image_transparent_color(image, transparent_color, threshold=1)
        if background_color_function is not None:
            color = background_color_function(i)
            image = image_background_color(image, color)
        if image.mode != "RGBA":
            image = image.convert("RGBA")
        location = (int((i % sprite_size) * size[0]), int((i // sprite_size) * size[1]))
        sprite_image.paste(image, location)
    return sprite_image


def image_background_color(image, color):
    # type: (Any, Sequence[int]) -> Optional[Any]
    """
    Given an image with some transparency, add a background color to it.

    Args:
        image: a PIL image
        color: a red, green, blue color tuple
    """
    try:
        from PIL import Image
    except ImportError:
        LOGGER.error("The Python library PIL is required for this operation")
        return None

    if image.mode != "RGBA":
        raise ValueError(
            "image must have an alpha channel in order to set a background color"
        )

    new_image = Image.new("RGB", image.size, color)
    new_image.paste(image, (0, 0), image)
    return new_image


def image_transparent_color(image, color, threshold=1):
    # type: (Any, Tuple[int, int, int], int) -> Any
    """
    Given a color, make that be the transparent color.

    Args:
        image: a PIL image
        color: a red, green, blue color tuple
        threshold: the max difference in each color component
    """
    try:
        import numpy
        from PIL import Image
    except ImportError:
        LOGGER.error(
            "The Python libraries PIL and numpy are required for this operation"
        )
        return

    image = image.convert("RGBA")
    array = numpy.array(numpy.asarray(image))
    r, g, b, a = numpy.rollaxis(array, axis=-1)
    mask = (
        (numpy.abs(r - color[0]) < threshold)
        & (numpy.abs(g - color[1]) < threshold)
        & (numpy.abs(b - color[2]) < threshold)
    )
    array[mask, 3] = 0
    return Image.fromarray(array, mode="RGBA")


def image_to_fp(image, image_format):
    # type: (Any, str) -> IO[bytes]
    """
    Convert a PIL.Image into an in-memory file
    pointer.
    """
    fp = io.BytesIO()
    image.save(fp, format=image_format)  # save the content to fp
    fp.seek(0)
    return fp


def rescale_array(array, old_range, new_range, dtype):
    """
    Given a numpy array in an old_range, rescale it
    into new_range, and make it an array of dtype.
    """
    if not HAS_NUMPY:
        LOGGER.error("The Python library numpy is required for this operation")
        return

    old_min, old_max = old_range
    if array.min() < old_min or array.max() > old_max:
        ## truncate:
        array = numpy.clip(array, old_min, old_max)
    new_min, new_max = new_range
    old_delta = float(old_max - old_min)
    new_delta = float(new_max - new_min)
    if old_delta == 0:
        return ((array - old_min) + (new_min + new_max) / 2.0).astype(dtype)
    else:
        return (new_min + (array - old_min) * new_delta / old_delta).astype(dtype)


def table_to_fp(tabular_data, delim, headers):
    # type: (Any, str, Union[Sequence[str], bool]) -> IO
    if isinstance(headers, bool):
        add_headers = headers
    else:
        add_headers = True
    fp = StringIO()
    for row in tabular_data:
        if isinstance(row, (numbers.Number, six.string_types)):
            if add_headers:
                if isinstance(headers, bool):
                    fp.write('"column1"\n')
                else:
                    fp.write('"%s"\n' % (headers[0],))
                add_headers = False
            fp.write(str(row) + "\n")
        else:
            columns = flatten(row)
            if add_headers:
                for i, column in enumerate(columns):
                    if isinstance(headers, bool):
                        column = '"column%s"' % (i + 1)
                    else:
                        column = '"%s"' % (headers[i],)
                    if i == len(columns) - 1:
                        fp.write(column)
                    else:
                        fp.write(column + delim)
                fp.write("\n")
                add_headers = False
            for i, column in enumerate(columns):
                if i == len(columns) - 1:
                    fp.write(str(column))
                else:
                    fp.write(str(column) + delim)
            fp.write("\n")
    fp.seek(0)
    return fp


def data_to_fp(data):
    # type: (Union[bytes, str, Any]) -> Optional[IO]
    if isinstance(data, str):
        fp = StringIO()
        fp.write(data)
    elif isinstance(data, bytes):
        fp = io.BytesIO()
        fp.write(data)
    else:
        fp = StringIO()
        try:
            json.dump(data, fp)
        except Exception:
            LOGGER.error("Failed to log asset data as JSON", exc_info=True)
            return None
    fp.seek(0)
    return fp


def write_numpy_array_as_wav(numpy_array, sample_rate, file_object):
    # type: (Any, int, IO) -> None
    """Convert a numpy array to a WAV file using the given sample_rate and
    write it to the file object
    """
    try:
        import numpy
        from scipy.io.wavfile import write
    except ImportError:
        LOGGER.error(
            "The Python libraries numpy, and scipy are required for this operation"
        )
        return

    array_max = numpy.max(numpy.abs(numpy_array))

    scaled = numpy.int16(numpy_array / array_max * 32767)

    write(file_object, sample_rate, scaled)


def lazy_flatten(iterable):
    if hasattr(iterable, "flatten"):
        iterable = iterable.flatten()
    iterator, sentinel, stack = iter(iterable), object(), []
    while True:
        value = next(iterator, sentinel)
        if value is sentinel:
            if not stack:
                break
            iterator = stack.pop()
        elif isinstance(value, (numbers.Number, six.string_types)):
            yield value
        else:
            if hasattr(value, "flatten"):
                value = value.flatten()  # type: ignore
            try:
                new_iterator = iter(value)
            except TypeError:
                yield value
            else:
                stack.append(iterator)
                iterator = new_iterator


def flatten(items):
    """
    Given a nested list or a numpy array,
    return the data flattened.
    """
    if isinstance(items, (numbers.Number, six.string_types)):
        return items
    return list(lazy_flatten(items))


def fast_flatten(items):
    """
    Given a nested list or a numpy array,
    return the data flattened.
    """
    if isinstance(items, (numbers.Number, six.string_types)):
        return items

    try:
        items = convert_tensor_to_numpy(items)
    except Exception:
        LOGGER.debug("unable to convert tensor; continuing", exc_info=True)

    if HAS_NUMPY:
        try:
            # Vector, Matrix conversion:
            items = numpy.array(items, dtype=float)
            # Return numpy array:
            return items.reshape(-1)
        except Exception:
            try:
                # Uneven conversion, 2 deep:
                items = numpy.array([numpy.array(item) for item in items], dtype=float)
                return items.reshape(-1)
            except Exception:
                # Fall through
                LOGGER.debug(
                    "numpy unable to convert items in fast_flatten", exc_info=True
                )
                return numpy.array(flatten(items))
    else:
        log_once_at_level(
            logging.INFO,
            "numpy not installed; using a slower flatten",
        )
        return flatten(items)


def convert_dict_to_string(user_data):
    # type: (Dict) -> str
    try:
        return json.dumps(user_data, sort_keys=True)
    except TypeError:
        retval = {}
        for key in user_data:
            try:
                value = convert_to_string(user_data[key])
            except Exception:
                value = str(user_data[key])
            retval[key] = value
        return str(retval)


def convert_to_string(user_data, source=None):
    # type: (Any, Optional[str]) -> str
    """
    Given an object, return it as a string.
    """
    if isinstance(user_data, Mapping):
        return convert_dict_to_string(user_data)

    # hydra ConfigNode special case:
    if hasattr(user_data, "node"):
        return convert_dict_to_string(user_data.node)

    if hasattr(user_data, "numpy"):
        user_data = convert_tensor_to_numpy(user_data)

    if isinstance(user_data, bytes) and not isinstance(user_data, str):
        user_data = user_data.decode("utf-8")

    if source is not None:
        try:
            user_data_repr = repr(user_data)
            LOGGER.warning(
                "Converting %s '%s' into a string using str(), resulting string might be invalid",
                source,
                user_data_repr,
            )
        except Exception:
            LOGGER.debug("Cannot get user_data repr", exc_info=True)
            LOGGER.warning(
                "Converting %s '%s' into a string using str(), resulting string might be invalid",
                source,
                user_data,
            )

    return str(user_data)


def convert_to_string_truncated(user_data, size, source=None):
    # type: (Any, int, Optional[str]) -> str
    value = convert_to_string(user_data, source)
    if len(value) > size:
        LOGGER.warning("truncated string; too long: '%s'...", value)
        indicator = " [truncated]"
        if size < len(indicator):
            value = value[:size]
        else:
            value = value[: size - len(indicator)] + indicator
    return value


def convert_to_string_key(user_data):
    # type: (Any) -> str
    return convert_to_string_truncated(user_data, 100)


def convert_to_string_value(user_data, source=None):
    # type: (Any, Optional[str]) -> str
    return convert_to_string_truncated(user_data, 1000, source=source)


def convert_model_to_string(model):
    # type: (Any) -> str
    """
    Given a model of some kind, convert to a string.
    """
    if type(model).__name__ == "Graph":  # Tensorflow Graph Definition
        try:
            from google.protobuf import json_format

            graph_def = model.as_graph_def()
            model = json_format.MessageToJson(graph_def, sort_keys=True)
        except Exception:
            LOGGER.warning("Failed to convert Tensorflow graph to JSON", exc_info=True)

    if hasattr(model, "to_json"):
        # First, try with sorted keys:
        try:
            model = model.to_json(sort_keys=True)
        except Exception:
            model = model.to_json()
    elif hasattr(model, "to_yaml"):
        model = model.to_yaml()

    try:
        return str(model)
    except Exception:
        LOGGER.warning("Unable to convert model to a string")
        return "Unable to convert model to a string"


def convert_object_to_dictionary(obj):
    # type: (Any) -> Dict[str, str]
    """
    This function takes an object and turns it into
    a dictionary. It turns all properties (including
    computed properties) into a {property_name: string, ...}
    dictionary.
    """
    # hydra ConfigStore special case:
    if obj.__class__.__module__.startswith("hydra.core") and hasattr(obj, "repo"):
        return obj.repo

    dic = {}
    for attr in dir(obj):
        # Python 2 exposed some "internal" functions attributed as `func_X`. They were renamed in
        # `__X__` in Python 3.
        if attr.startswith("__") or attr.startswith("to_") or attr.startswith("func_"):
            continue
        value = getattr(obj, attr)
        if callable(value):
            continue
        try:
            dic[attr] = str(value)
        except Exception:
            pass
    return dic


def prepare_dataframe(dataframe, asset_format, **kwargs):
    # type: (Any, Optional[str], Optional[dict]) -> Optional[StringIO]
    """
    Log a pandas dataframe.
    """
    fp = StringIO()
    if asset_format == "json":
        dataframe.to_json(fp, **kwargs)
    elif asset_format == "csv":
        dataframe.to_csv(fp, **kwargs)
    elif asset_format == "md":
        dataframe.to_markdown(fp, **kwargs)
    elif asset_format == "html":
        dataframe.to_html(fp, **kwargs)
    else:
        LOGGER.warning(
            "invalid asset_format %r; should be 'json', "
            + "'csv', 'md', or 'html'; ignoring",
            asset_format,
        )
        return None

    fp.seek(0)
    return fp


def check_is_pandas_dataframe(dataframe):
    # type: (Any) -> Optional[bool]
    """
    Is it like a dataframe? For example, does it have
    to_markdown(), to_html() methods?
    """
    try:
        from pandas.core.dtypes.generic import ABCDataFrame, ABCSeries
    except ImportError:
        return None

    return isinstance(dataframe, (ABCDataFrame, ABCSeries))


def convert_log_table_input_to_io(
    filename, tabular_data, headers, format_kwargs, catch_exception=True
):
    # type: (str, Optional[Any], Union[Sequence[str], bool], Dict[str, Any], bool) -> Optional[Tuple[IO, str]]

    # Get the extension
    if "." in filename:
        format = filename.rsplit(".", 1)[-1]
    else:
        format = ""

    if check_is_pandas_dataframe(tabular_data):
        if format not in ["json", "csv", "md", "html"]:
            if catch_exception:
                LOGGER.error(CONVERT_DATAFRAME_INVALID_FORMAT, format)
                return None
            else:
                raise ValueError(CONVERT_DATAFRAME_INVALID_FORMAT % format)

        try:
            dataframe_fp = prepare_dataframe(tabular_data, format, **format_kwargs)
        except Exception:
            if catch_exception is False:
                raise

            LOGGER.error(DATAFRAME_CONVERSION_ERROR, format, exc_info=True)
            return None

        if dataframe_fp:
            return (dataframe_fp, "dataframe")
        else:
            return None

    else:
        if format not in ["tsv", "csv"]:
            if catch_exception:
                LOGGER.error(CONVERT_TABLE_INVALID_FORMAT, format)
                return None
            else:
                raise ValueError(CONVERT_TABLE_INVALID_FORMAT % format)

        delim = ""
        if format == "tsv":
            delim = "\t"
        elif format == "csv":
            delim = ","
        fp = table_to_fp(tabular_data, delim, headers)
        return (fp, "asset")


def convert_user_input_to_metric_value(user_input):
    # type: (Any) -> Union[int, float, str]
    value = convert_to_scalar(user_input)

    if is_list_like(value):
        # Try to get the first value of the Array
        try:
            if len(value) != 1:
                raise TypeError()

            if not isinstance(value[0], (six.integer_types, float)) or isinstance(
                value, bool
            ):
                raise TypeError()

            value = value[0]

        except TypeError:
            LOGGER.warning(METRIC_ARRAY_WARNING, value)

            value = convert_to_string_value(value)
    else:
        if not isinstance(value, (six.integer_types, float)) or isinstance(value, bool):
            value = convert_to_string_value(value, source="metric value")

    return value
