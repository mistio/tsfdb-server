# coding: utf-8

from __future__ import absolute_import
import unittest

from flask import json
from six import BytesIO

from tsfdb_server_v1.models.datapoints_response import DatapointsResponse  # noqa: E501
from tsfdb_server_v1.models.error import Error  # noqa: E501
from tsfdb_server_v1.test import BaseTestCase


class TestDatapointsController(BaseTestCase):
    """DatapointsController integration test stubs"""

    def test_fetch_datapoints(self):
        """Test case for fetch_datapoints

        Return datapoints within a given time range for given resources & metric name patterns
        """
        query_string = [('query', 'query_example')]
        headers = { 
            'Accept': 'application/json',
        }
        response = self.client.open(
            '/v1/datapoints',
            method='GET',
            headers=headers,
            query_string=query_string)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
