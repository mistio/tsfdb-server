import asyncio
import connexion
import re
import numpy as np
import logging
import json
from .helpers import round_base, error
from .db import async_fetch_list
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
        async_fetch_list(
            org, multiple_resources_and_metrics, start, stop, step,
            authorized_resources))

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


def topk(data, k=20):
    if not isinstance(data, dict) or not data:
        return {}
    sum_data = {}
    top_data = {}
    for metric, datapoints in data.items():
        sum_data[metric] = sum(x for x, y in datapoints)/len(datapoints)
    for metric, datapoints in sorted(sum_data.items(),
                                     key=lambda item: item[1],
                                     reverse=True)[0:k]:
        top_data[metric] = data[metric]
    return top_data
