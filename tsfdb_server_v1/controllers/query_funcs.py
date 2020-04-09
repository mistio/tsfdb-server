import asyncio
import re
import numpy as np
import logging
from .helpers import round_base, is_regex, error
from .db import find_metrics, find_datapoints
from tsfdb_server_v1.models.error import Error  # noqa: E501

log = logging.getLogger(__name__)


def roundX(data, precision=0, base=1):
    if not isinstance(data, dict) or not data:
        return {}
    for metric, datapoints in data.items():
        data[metric] = [
            [round_base(x, precision, base), y]
            for x, y in datapoints
        ]
    return data


def roundY(data, precision=0, base=1):
    if not isinstance(data, dict) or not data:
        return {}
    for metric, datapoints in data.items():
        data[metric] = [
            [x, round_base(y, precision, base)]
            for x, y in datapoints
        ]
    return data


def fetch(resources_and_metrics, start="", stop="", step=""):
    # We take for granted that all metrics start with the id and that
    # it ends on the first occurence of a dot, e.g id.system.load1
    data = {}
    resources, metrics = resources_and_metrics.split(".", 1)
    if is_regex(resources):
        # At the moment we ignore the cases where the resource is a regex
        # resources = find_resources(resources)
        return {}
    else:
        resources = [resources]

    for resource in resources:
        if is_regex(metrics):
            regex_metric = metrics
            metrics = []
            all_metrics = find_metrics(resource)
            if isinstance(all_metrics, Error):
                return all_metrics
            if regex_metric == "*":
                metrics = [candidate for candidate in all_metrics]
            else:
                for candidate in all_metrics:
                    if re.match("^%s$" % regex_metric, candidate):
                        metrics.append(candidate)
            if len(metrics) == 0:
                error_msg = (
                    "No metrics for regex: \"%s\" where found" % regex_metric
                )
                return error(400, error_msg, log)
        else:
            metrics = [metrics]
        loop = asyncio.get_event_loop()
        current_data = loop.run_until_complete(
            find_datapoints(resource, start, stop, metrics))
        if isinstance(current_data, Error):
            return current_data
        data.update(current_data)

    return data


def deriv(data):
    if not isinstance(data, dict) or not data:
        return {}
    for metric, datapoints in data.items():
        if not datapoints or len(datapoints) < 2:
            data[metric] = []
            continue
        values = np.asarray([value for value, _ in datapoints])
        timestamps = np.asarray([timestamp for _, timestamp in datapoints])
        values_deriv = np.gradient(values, timestamps)
        data[metric] = [
            [value_deriv.tolist(), timestamp.tolist()]
            for value_deriv, timestamp in zip(
                np.nditer(values_deriv), np.nditer(timestamps)
            )
        ]
    return data
