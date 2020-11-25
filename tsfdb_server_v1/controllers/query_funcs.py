import asyncio
import connexion
import re
import numpy as np
import logging
import json
from .helpers import round_base, error, parse_relative_time_to_seconds, \
    parse_start_stop_params
from datetime import datetime
from .db import DBOperations
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


def mean(data):
    if not isinstance(data, dict) or not data:
        return {}
    for metric, datapoints in data.items():
        mean_data = {}
        for value, timestamp in datapoints:
            if not mean_data.get(timestamp):
                mean_data[timestamp] = []
            mean_data[timestamp].append(value)
        data[metric] = []
        for timestamp, values in mean_data.items():
            data[metric].append([sum(values)/len(values), timestamp])
    return data


def fetch(db_ops, resources_and_metrics, start="", stop="", step=""):
    # We take for granted that all metrics start with the id and that
    # it ends on the first occurence of a dot, e.g id.system.load1
    start, stop = parse_start_stop_params(start, stop)
    if start > stop:
        return Error(code=400, message="Invalid time range")
    start = str(int(datetime.timestamp(start)))
    stop = str(int(datetime.timestamp(stop)))
    data = {}
    org = connexion.request.headers['x-org-id']
    authorized_resources = connexion.request.headers.get('x-allowed-resources')
    if authorized_resources:
        authorized_resources = json.loads(authorized_resources)

    if isinstance(resources_and_metrics, str):
        multiple_resources_and_metrics = [resources_and_metrics]
    else:
        multiple_resources_and_metrics = resources_and_metrics

    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(
        db_ops.async_fetch_list(
            org, multiple_resources_and_metrics, start, stop,
            authorized_resources))
    if not isinstance(data, Error) and step:
        return mean(roundY(data, base=parse_relative_time_to_seconds(step)))
    return data


def fetch_monitoring(resources_and_metrics, start="", stop="", step="",
                     resolution=""):
    db_ops = DBOperations(resolution=resolution)
    return fetch(db_ops, resources_and_metrics, start, stop, step)


def fetch_metering(resources_and_metrics, start="", stop="", step="",
                   resolution=""):
    db_ops = DBOperations(series_type="metering", resolution=resolution)
    return fetch(db_ops, resources_and_metrics, start, stop, step)


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


def topk(data, k=20):
    if not isinstance(data, dict) or not data:
        return {}
    sum_data = {}
    top_data = {}
    for metric, datapoints in data.items():
        if datapoints:
            sum_data[metric] = sum(x for x, y in datapoints)/len(datapoints)
        else:
            sum_data[metric] = 0
    for metric, datapoints in sorted(sum_data.items(),
                                     key=lambda item: item[1],
                                     reverse=True)[0:k]:
        top_data[metric] = data[metric]
    return top_data
