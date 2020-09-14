import connexion
import six

from tsfdb_server_v1.models.error import Error  # noqa: E501
from tsfdb_server_v1 import util
from tsfdb_server_v1.controllers.internal_metrics import InternalMetrics
from prometheus_client import CollectorRegistry, Gauge
from prometheus_client.exposition import _bake_output


def list_internal_metrics():  # noqa: E501
    """Return metrics and metadata for a specific resource

     # noqa: E501

    :rtype: str
    """
    registry = CollectorRegistry()
    internal_metrics = InternalMetrics().get_all()

    for k, v in internal_metrics.items():
        g = Gauge(k.replace(".", "__").replace(
            "-", "_"), '', registry=registry)
        g.set(v)
    _, _, output = _bake_output(registry, '', {})
    return output.decode("utf-8")
