# coding: utf-8

from __future__ import absolute_import
import unittest
import fdb
import fdb.tuple

from flask import json
from six import BytesIO
from tqdm import tqdm
from datetime import datetime
from random import random

from tsfdb_server_v1.models.datapoints_response import DatapointsResponse  # noqa: E501
from tsfdb_server_v1.models.error import Error  # noqa: E501
from tsfdb_server_v1.test import BaseTestCase
from tsfdb_server_v1.controllers.helpers import create_key_tuple_second, \
    create_key_tuple_minute, create_key_tuple_hour, create_key_tuple_day
from tsfdb_server_v1.test.helpers import validate_aggregation

fdb.api_version(620)


class TestDatapointsController(BaseTestCase):
    """DatapointsController integration test stubs"""

    def test_fetch_datapoints(self):
        """Test case for fetch_datapoints

        Return datapoints within a given time range for given resources & metric name patterns
        """
        query_string = [('query', 'fetch(\"123456789.system.load1\")',)]
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

    def test_write_datapoints(self):
        """Test case for write_datapoints

        Write datapoints to db
        """
        db = fdb.open()
        # Clear db
        del db[b'':b'\xff']
        timestamp = 1581007254
        # Test for data from 2 days
        body = ""
        dump_per = 100
        stop = 48 * 60 * 60 + 1

        # Test aggregation with every value = 1.0

        for i, delta in enumerate(tqdm(range(0, stop, 5), desc="Writing datapoints to fdb ..."),
                                  start=1):

            body += ("system,machine_id=123456789 load1=%s %s\n" %
                     (str(1.0), str(timestamp + delta)))
            if i % dump_per == 0:
                response = self.client.open(
                    '/v1/datapoints',
                    method='POST',
                    data=body)
                self.assertStatus(response, 204, message=None)
                body = ""
        validate_aggregation(timestamp, stop)

        # Test aggregation with random values

        del db[b'':b'\xff']

        body = ""

        for i, delta in enumerate(tqdm(range(0, stop, 5), desc="Writing datapoints to fdb ..."),
                                  start=1):
            body += ("system,machine_id=123456789 load1=%s %s\n" %
                     (str(random()), str(timestamp + delta)))
            if i % dump_per == 0:
                response = self.client.open(
                    '/v1/datapoints',
                    method='POST',
                    data=body)
                self.assertStatus(response, 204, message=None)
                body = ""

        validate_aggregation(timestamp, stop)

        # Test writing the same key with the same value

        body = 2*("system,machine_id=123456789 load1=%s %s\n" %
                  (str(random()), str(timestamp + delta)))
        response = self.client.open(
            '/v1/datapoints',
            method='POST',
            data=body)
        self.assertStatus(response, 204, message=None)

        del db[b'':b'\xff']


if __name__ == '__main__':
    unittest.main()
