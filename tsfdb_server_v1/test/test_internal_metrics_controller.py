# coding: utf-8

from __future__ import absolute_import
import unittest

from flask import json
from six import BytesIO

from tsfdb_server_v1.test import BaseTestCase


class TestInternalMetricsController(BaseTestCase):
    """InternalMetricsController integration test stubs"""

    def test_list_internal_metrics(self):
        """Test case for list_internal_metrics

        Return internal metrics and metadata for a specific resource
        """
        headers = { 
            'Accept': 'text/plain',
        }
        response = self.client.open(
            '/v1/internal_metrics',
            method='GET',
            headers=headers)
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    unittest.main()
