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
#  This file can not be copied and/or distributed
#  without the express permission of Comet ML Inc.
# *******************************************************

import logging
import numbers
from datetime import datetime

from .exceptions import QueryException

__all__ = ["Environment", "Metadata", "Metric", "Other", "Parameter", "Tag"]


LOGGER = logging.getLogger(__name__)


class QueryExpression(object):
    def __init__(self, lhs, op, rhs=None):
        self.lhs = lhs
        self.op = op
        if isinstance(rhs, datetime):
            self.rhs = rhs.isoformat(" ")
        elif hasattr(rhs, "decode"):
            ok = True
            try:
                self.rhs = rhs.decode("utf-8")
            except Exception:
                LOGGER.debug("Error decoding value", exc_info=True)
                ok = False

            if not ok:
                raise ValueError("invalid expression %r: can't decode to unicode", rhs)
        else:
            self.rhs = rhs

    def __bool__(self):
        # For Python 3:
        raise TypeError(
            "can't use logical operators; use '&' for an AND query and '~' to invert your query. OR queries are not yet supported"
        )

    def __nonzero__(self):
        # For Python 2:
        raise TypeError(
            "can't use logical operators; use '&' for an AND query and '~' to invert your query. OR queries are not yet supported"
        )

    def __and__(self, rhs):
        return QueryExpression(self, "&", rhs)

    def __or__(self, rhs):
        raise ValueError("queries cannot currently handle '|' (mathematical or)")

    def __eq__(self, rhs):
        raise ValueError(
            "illegal operator on a query expression; use parentheses around expressions"
        )

    def __ne__(self, rhs):
        raise ValueError(
            "illegal operator on a query expression; use parentheses around expressions"
        )

    def __lt__(self, rhs):
        raise ValueError(
            "illegal operator on a query expression; use parentheses around expressions"
        )

    def __le__(self, rhs):
        raise ValueError(
            "illegal operator on a query expression; use parentheses around expressions"
        )

    def __gt__(self, rhs):
        raise ValueError(
            "illegal operator on a query expression; use parentheses around expressions"
        )

    def __ge__(self, rhs):
        raise ValueError(
            "illegal operator on a query expression; use parentheses around expressions"
        )

    def __invert__(self):
        if isinstance(self.lhs, _Tag):
            raise ValueError("can't negate a Tag expression")
        # mathematical (6, including negatives):
        if self.op == "equal":
            return QueryExpression(self.lhs, "not_equal", self.rhs)
        elif self.op == "not_equal":
            return QueryExpression(self.lhs, "equal", self.rhs)
        elif self.op == "less":
            return QueryExpression(self.lhs, "greater_or_equal", self.rhs)
        elif self.op == "greater_or_equal":
            return QueryExpression(self.lhs, "less", self.rhs)
        elif self.op == "greater":
            return QueryExpression(self.lhs, "less_or_equal", self.rhs)
        elif self.op == "less_or_equal":
            return QueryExpression(self.lhs, "greater", self.rhs)
        # special (6 positive, 6 negatives == 12):
        elif self.op == "between":
            return QueryExpression(self.lhs, "not_between", self.rhs)
        elif self.op == "not_between":
            return QueryExpression(self.lhs, "between", self.rhs)
        elif self.op == "contains":
            return QueryExpression(self.lhs, "not_contains", self.rhs)
        elif self.op == "not_contains":
            return QueryExpression(self.lhs, "contains", self.rhs)
        elif self.op == "begins_with":
            return QueryExpression(self.lhs, "not_begins_with", self.rhs)
        elif self.op == "not_begins_with":
            return QueryExpression(self.lhs, "begins_with", self.rhs)
        elif self.op == "ends_with":
            return QueryExpression(self.lhs, "not_ends_with", self.rhs)
        elif self.op == "not_ends_with":
            return QueryExpression(self.lhs, "ends_with", self.rhs)
        elif self.op == "is_null":
            return QueryExpression(self.lhs, "is_not_null", self.rhs)
        elif self.op == "is_not_null":
            return QueryExpression(self.lhs, "is_null", self.rhs)
        elif self.op == "is_empty":
            return QueryExpression(self.lhs, "is_not_empty", self.rhs)
        elif self.op == "is_not_empty":
            return QueryExpression(self.lhs, "is_empty", self.rhs)
        elif self.op is None:
            raise ValueError("can't negate this expression: %s" % repr(self))
        else:
            raise ValueError("unknown operator: %s" % self.op)

    def __repr__(self):
        if self.op:
            pp = {
                "equal": "==",
                "not_equal": "!=",
                "less": "<",
                "less_or_equal": "<=",
                "greater": ">",
                "greater_or_equal": ">=",
                "is_empty": "==",
                "is_not_empty": "!=",
                "is_null": "==",
                "is_not_null": "!=",
            }
            return "(%r %s %r)" % (self.lhs, pp.get(self.op, self.op), self.rhs)
        else:
            return repr(self.lhs)

    def _get_qtype(self, columns):
        # return string, boolean, double, datetime, timenumber
        for column_data in columns["columns"]:
            if (
                column_data["name"] == self.lhs.name
                and column_data["source"] == self.lhs.source
            ):
                return column_data["type"]
        raise QueryException("no such %s: %r" % (self.lhs.source, self.lhs.name))

    def _verify_qtype(self, qtype):
        if self.op in ["begins_with", "not_begins_with"]:
            if qtype != "string":
                raise ValueError(
                    "QueryVariable.startswith() requires that QueryVariable be a string type not %r"
                    % qtype
                )
        elif self.op in ["ends_with", "not_ends_with"]:
            if qtype != "string":
                raise ValueError(
                    "QueryVariable.endswith() requires that QueryVariable be a string type not %r"
                    % qtype
                )
        elif self.op in ["contains", "not_contains"]:
            if qtype != "string":
                raise ValueError(
                    "QueryVariable.contains() requires that QueryVariable be a string type not %r"
                    % qtype
                )
        elif self.op in ["between", "not_between"]:
            if qtype == "string":
                raise ValueError(
                    "QueryVariable.between() requires that QueryVariable be a numeric type not %r"
                    % qtype
                )
        # else, query is pretty leniant on type matching

    def _get_rules(self, columns):
        if self.lhs.qtype is None:
            self.lhs.qtype = self._get_qtype(columns)
        self._verify_qtype(self.lhs.qtype)
        rule = {
            "id": self.lhs.name,
            "field": self.lhs.name,
            "type": self.lhs.qtype,
            "operator": self.op,
            "value": self.rhs,
        }
        return [rule]

    def get_predicates(self, columns):
        if self.op == "&":
            if isinstance(self.lhs, QueryVariable):
                raise ValueError(
                    "invalid query expression on left: %r; you need to compare this value"
                    % (self.lhs,)
                )
            elif not isinstance(self.lhs, QueryExpression):
                raise ValueError(
                    "invalid query expression on left: %r; do not use 'and', 'or', 'not', 'is', or 'in'"
                    % (self.lhs,)
                )
            if isinstance(self.rhs, QueryVariable):
                raise ValueError(
                    "invalid query expression on right: %r; you need to compare this value"
                    % (self.rhs,)
                )
            elif not isinstance(self.rhs, QueryExpression):
                raise ValueError(
                    "invalid query expression on right: %r; do not use 'and', 'or', 'not', 'is', or 'in'"
                    % (self.rhs,)
                )
            lhs_predicates = self.lhs.get_predicates(columns)
            rhs_predicates = self.rhs.get_predicates(columns)
            # Combine:
            for predicate in rhs_predicates:
                lhs_sources = [pred["source"] for pred in lhs_predicates]
                if predicate["source"] in lhs_sources:
                    index = lhs_sources.index(predicate["source"])
                    # combine rules:
                    lhs_predicates[index]["query"]["rules"].extend(
                        predicate["query"]["rules"]
                    )
                else:  # add to predicates:
                    lhs_predicates.append(predicate)
            return lhs_predicates
        else:
            return [
                {
                    "source": self.lhs.source,
                    "query": {
                        "condition": "AND",
                        "rules": self._get_rules(columns),
                        "valid": True,
                    },
                }
            ]


class QueryVariable(object):
    def __init__(self, name, qtype=None):
        self.name = name
        self.qtype = qtype

    def __bool__(self):
        # For Python 3:
        raise TypeError("can't use logical operators on a QueryVariable")

    def __nonzero__(self):
        # For Python 2:
        raise TypeError("can't use logical operators on a QueryVariable")

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.name)

    def __contains__(self, rhs):
        raise ValueError("can't use 'in' operator in queries; use X.contains(Y)")

    def contains(self, rhs):
        if not isinstance(rhs, str):
            raise ValueError(
                "QueryVariable.contains(X) requires that X be a string type"
            )
        return QueryExpression(self, "contains", rhs)

    def between(self, low, high):
        if not isinstance(low, numbers.Number) or not isinstance(high, numbers.Number):
            raise ValueError(
                "QueryVariable.between(low, high) requires that low and high be numbers"
            )
        return QueryExpression(self, "between", [str(low), str(high)])

    def startswith(self, rhs):
        if not isinstance(rhs, str):
            raise ValueError(
                "QueryVariable.startswith(X) requires that X be a string type"
            )
        return QueryExpression(self, "begins_with", rhs)

    def endswith(self, rhs):
        if not isinstance(rhs, str):
            raise ValueError(
                "QueryVariable.endswith(X) requires that X be a string type"
            )
        return QueryExpression(self, "ends_with", rhs)

    def __eq__(self, rhs):
        if rhs is None:
            return QueryExpression(self, "is_null", None)
        elif rhs == "":
            return QueryExpression(self, "is_empty", None)
        elif rhs is True:
            return QueryExpression(self, "equal", 1)
        elif rhs is False:
            return QueryExpression(self, "equal", 0)
        else:
            return QueryExpression(self, "equal", rhs)

    def __ne__(self, rhs):
        if rhs is None:
            return QueryExpression(self, "is_not_null", None)
        elif rhs == "":
            return QueryExpression(self, "is_not_empty", None)
        elif rhs is True:
            return QueryExpression(self, "not_equal", 1)
        elif rhs is False:
            return QueryExpression(self, "not_equal", 0)
        else:
            return QueryExpression(self, "not_equal", rhs)

    def __lt__(self, rhs):
        return QueryExpression(self, "less", rhs)

    def __le__(self, rhs):
        return QueryExpression(self, "less_or_equal", rhs)

    def __gt__(self, rhs):
        return QueryExpression(self, "greater", rhs)

    def __ge__(self, rhs):
        return QueryExpression(self, "greater_or_equal", rhs)


class Metric(QueryVariable):
    """
    Create a QueryVariable for querying metrics.

    Args:
        name: String, name of the log_metric() item

    Returns: a `QueryVariable` to be used with `API.query()`
        to match the experiments

    Examples:

    ```python
    >>> from comet_ml.api import API, Metric
    >>> api = API()
    >>> api.query("workspace", "project", Metric("accuracy") > 0.9)
    ```

    Note: you must always use a query operator with a `QueryVariable`,
        such as `==`, `<`, or `QueryVariable.contains("substring")`
    """

    source = "metrics"


class Parameter(QueryVariable):
    """
    Create a QueryVariable for querying parameters.

    Args:
        name: String, name of the log_parameter() item

    Returns: a `QueryVariable` to be used with `API.query()`
        to match the experiments

    Examples:

    ```python
    >>> from comet_ml.api import API, Parameter
    >>> api = API()
    >>> api.query("workspace", "project", Parameter("learning rate") >= 1.2)
    ```

    Note: you must always use a query operator with a `QueryVariable`,
        such as `==`, `<`, or `QueryVariable.contains("substring")`
    """

    source = "params"


class Metadata(QueryVariable):
    """
    Create a QueryVariable for querying metadata.

    Args:
        name: String, name of the metadata item

    Returns: a `QueryVariable` to be used with `API.query()`
        to match the experiments

    Examples:

    ```python
    >>> from comet_ml.api import API, Metadata
    >>> api = API()
    >>> api.query("workspace", "project", Metadata("name") == "value")
    ```

    Note: you must always use a query operator with a `QueryVariable`,
        such as `==`, `<`, or `QueryVariable.contains("substring")`
    """

    source = "metadata"


class Environment(QueryVariable):
    """
    Create a QueryVariable for querying environment details.

    Args:
        name: String, name of the environment item

    Returns: a `QueryVariable` to be used with `API.query()`
        to match the experiments

    Examples:

    ```python
    >>> from comet_ml.api import API, Environment
    >>> api = API()
    >>> api.query("workspace", "project", Environment("os") == "darwin")
    ```

    Note: you must always use a query operator with a `QueryVariable`,
        such as `==`, `<`, or `QueryVariable.contains("substring")`
    """

    source = "env_details"


class Other(QueryVariable):
    """
    Create a QueryVariable for querying logged-others.

    Args:
        name: String, name of the log_other() item

    Returns: a `QueryVariable` to be used with `API.query()`
        to match the experiments

    Examples:

    ```python
    >>> from comet_ml.api import API, Other
    >>> api = API()
    >>> api.query("workspace", "project", Other("other name") == "value")
    ```

    Note: you must always use a query operator with a `QueryVariable`,
        such as `==`, `<`, or `QueryVariable.contains("substring")`
    """

    source = "log_other"


class _Tag(QueryVariable):
    source = "tag"

    def __repr__(self):
        return "Tag(%r)" % (self.name,)


class Tag(QueryExpression):
    """
    Create a QueryExpression for querying tags.

    Args:
        name: String, name of tag

    Returns: a `QueryExpression` to be used with `API.query()`
        to match the experiments with this tag

    Examples:

    ```python
    >>> from comet_ml.api import API, Tag
    >>> api = API()
    >>> api.query("workspace", "project", Tag("tag name"))
    ```

    Note: if used on a project that does not contain any items
        with this tag, then a warning will appear and no items
        will match.
    """

    def __init__(self, name):
        self.lhs = _Tag(name, qtype="string")
        self.op = "equal"
        self.rhs = name

    def __repr__(self):
        return "Tag(%r)" % (self.rhs,)
