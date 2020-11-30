import connexion
import six

from RestrictedPython import compile_restricted
from RestrictedPython import safe_builtins
from tsfdb_server_v1.models.datapoints_response import DatapointsResponse  # noqa: E501
from tsfdb_server_v1.models.error import Error  # noqa: E501
from tsfdb_server_v1 import util
from .db import DBOperations
from .query_funcs import deriv, roundX, roundY, topk, mean
from .query_funcs import fetch_metering as fetch


def fetch_metering_datapoints(query, x_org_id, x_allowed_resources=None):  # noqa: E501
    """Return metering datapoints within a given time range for given resources &amp; metric name patterns

     # noqa: E501

    :param query: Comma seperated id&#39;s of the metrics to retrieve datapoints for
    :type query: str
    :param x_org_id: Organization id
    :type x_org_id: str
    :param x_allowed_resources: Allowed resources
    :type x_allowed_resources: List[str]

    :rtype: DatapointsResponse
    """
    funcs = {"fetch": fetch, "deriv": deriv, "roundX": roundX,
             "roundY": roundY, "topk": topk, "mean": mean}
    allowed_params = {'__builtins__': safe_builtins}.update(funcs)
    try:
        byte_code = compile_restricted(
            query,
            filename='<inline code>',
            mode='eval'
        )
        data = eval(byte_code, allowed_params)
    except SyntaxError as e:
        log.error("Error when parsing query: %s, error: %s", query, str(e))
        return Error(400, "Bad request")

    if isinstance(data, Error):
        return data
    else:
        return DatapointsResponse(query=str(query), series=data)


def write_metering_datapoints(x_org_id, body):  # noqa: E501
    """Write metering datapoints to db

     # noqa: E501

    :param x_org_id: Organization id
    :type x_org_id: str
    :param body: Datapoints object to write
    :type body: str

    :rtype: None
    """
    db_ops = DBOperations(series_type="metering")
    body = str(body, 'utf8')
    db_ops.write_in_kv(x_org_id, body)
