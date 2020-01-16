import connexion
import six

from tsfdb_server_v1.models.error import Error  # noqa: E501
from tsfdb_server_v1.models.resource import Resource  # noqa: E501
from tsfdb_server_v1 import util


def list_metrics_by_resource(resource_id):  # noqa: E501
    """Return metrics and metadata for a specific resource

     # noqa: E501

    :param resource_id: The id of the resource to retrieve
    :type resource_id: str

    :rtype: Resource
    """
    return 'do some magic!'