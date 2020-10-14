import connexion
import six

from tsfdb_server_v1.models.datapoints_response import DatapointsResponse  # noqa: E501
from tsfdb_server_v1.models.error import Error  # noqa: E501
from tsfdb_server_v1 import util


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
    return 'do some magic!'


def write_metering_datapoints(x_org_id, body):  # noqa: E501
    """Write metering datapoints to db

     # noqa: E501

    :param x_org_id: Organization id
    :type x_org_id: str
    :param body: Datapoints object to write
    :type body: str

    :rtype: None
    """
    return 'do some magic!'
