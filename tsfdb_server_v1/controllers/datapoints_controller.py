import connexion
import six
import logging

from RestrictedPython import compile_restricted
from RestrictedPython import safe_builtins
from tsfdb_server_v1.models.datapoints_response import DatapointsResponse  # noqa: E501
from tsfdb_server_v1.models.error import Error  # noqa: E501
from tsfdb_server_v1 import util
from .query_funcs import fetch, deriv, roundX, roundY
from .db import write

log = logging.getLogger(__name__)


def fetch_datapoints(query):  # noqa: E501
    """Return datapoints within a given time range for given resources &amp; metric name patterns

     # noqa: E501

    :param query: Comma seperated id&#39;s of the metrics to retrieve datapoints for
    :type query: str

    :rtype: DatapointsResponse
    """

    allowed_funcs = {'__builtins__': safe_builtins,
                     "fetch": fetch, "deriv": deriv, "roundX": roundX,
                     "roundY": roundY}
    try:
        byte_code = compile_restricted(
            query,
            filename='<inline code>',
            mode='eval'
        )
        data = exec(byte_code, allowed_funcs, None)
    except SyntaxError as e:
        log.error("Error when parsing query: %s, error: %s", query, str(e))
        return Error(400, "Bad request")

    funcs = {"fetch": fetch, "deriv": deriv, "roundX": roundX,
             "roundY": roundY}
    code = compile(query, "query", "eval")
    data = eval(code, funcs)

    if isinstance(data, Error):
        return data
    else:
        return DatapointsResponse(query=str(query), series=data)


def write_datapoints(body, owner=None):  # noqa: E501
    """Write datapoints to db

     # noqa: E501

    :param body: Datapoints object to write
    :type body:
    :param owner:
    :type owner: str

    :rtype: None
    """
    body = str(body, 'utf8')
    write(body)
