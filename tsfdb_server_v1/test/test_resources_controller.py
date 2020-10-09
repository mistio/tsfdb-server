# coding: utf-8

from __future__ import absolute_import
import unittest

from flask import json
from six import BytesIO

from tsfdb_server_v1.models.error import Error  # noqa: E501
from tsfdb_server_v1.models.resource import Resource  # noqa: E501
from tsfdb_server_v1.test import BaseTestCase


class TestResourcesController(BaseTestCase):
    """ResourcesController integration test stubs"""

    def test_list_metrics_by_resource(self):
        """Test case for list_metrics_by_resource

        Return metrics and metadata for a specific resource
        """
        headers = { 
            'Accept': 'application/json',
            'x_org_id': 'x_org_id_example',
        }
        response = self.client.open(
            '/v1/resources/{resource_id}'.format(resource_id='resource_id_example'),
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_list_resources(self):
        """Test case for list_resources

        List all monitored resources
        """
        query_string = [('limit', 56)]
        headers = { 
            'Accept': 'application/json',
            'x_org_id': 'x_org_id_example',
        }
        response = self.client.open(
            '/v1/resources',
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
