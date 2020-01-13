# coding: utf-8

from __future__ import absolute_import
import unittest

from flask import json
from six import BytesIO

from tsfdb_server_v1.models.error import Error  # noqa: E501
from tsfdb_server_v1.models.resource import Resource  # noqa: E501
from tsfdb_server_v1.test import BaseTestCase


class TestMetricsController(BaseTestCase):
    """MetricsController integration test stubs"""

    def test_list_metrics_by_resource(self):
        """Test case for list_metrics_by_resource

        Return metrics and metadata for a specific resource
        """
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/resources/{resource_id}'.format(resource_id='resource_id_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
