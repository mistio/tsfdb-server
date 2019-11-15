import connexion
import six

from tsfdb_server_v1.models.datapoints_response import DatapointsResponse  # noqa: E501
from tsfdb_server_v1.models.error import Error  # noqa: E501
from tsfdb_server_v1 import util
from .helpers import fetch


def fetch_datapoints(query):  # noqa: E501
    """Return datapoints within a given time range for given resources &amp; metric name patterns

     # noqa: E501

    :param query: Comma seperated id&#39;s of the resources to retrieve datapoints for
    :type query: str

    :rtype: DatapointsResponse
    """
    funcs = {"fetch": fetch}
    code = compile(query, "query", "eval")
    data = eval(code, funcs)

    if isinstance(data, Error):
        return data
    else:
        return DatapointsResponse(query=str(query), series=data)
