import connexion
import six

from tsfdb_server_v1.models.datapoints_response import DatapointsResponse  # noqa: E501
from tsfdb_server_v1.models.error import Error  # noqa: E501
from tsfdb_server_v1 import util


def fetch_datapoints(query):  # noqa: E501
    """Return datapoints within a given time range for given resources &amp; metric name patterns

     # noqa: E501

    :param query: Comma seperated id&#39;s of the metrics to retrieve datapoints for
    :type query: str

    :rtype: DatapointsResponse
    """
    return 'do some magic!'


def write_datapoints(body):  # noqa: E501
    """Write datapoints to db

     # noqa: E501

    :param body: Datapoints object to write
    :type body: 

    :rtype: None
    """
    return 'do some magic!'
