# coding: utf-8

from __future__ import absolute_import
from datetime import date, datetime  # noqa: F401

from typing import List, Dict  # noqa: F401

from tsfdb_server_v1.models.base_model_ import Model
from tsfdb_server_v1.models.series_response import SeriesResponse
from tsfdb_server_v1 import util

from tsfdb_server_v1.models.series_response import SeriesResponse  # noqa: E501


class DatapointsResponse(Model):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.
    """

    def __init__(self, query=None, series=None, status=None):  # noqa: E501
        """DatapointsResponse - a model defined in OpenAPI

        :param query: The query of this DatapointsResponse.  # noqa: E501
        :type query: str
        :param series: The series of this DatapointsResponse.  # noqa: E501
        :type series: List[SeriesResponse]
        :param status: The status of this DatapointsResponse.  # noqa: E501
        :type status: str
        """
        self.openapi_types = {
            'query': str,
            'series': List[SeriesResponse],
            'status': str
        }

        self.attribute_map = {
            'query': 'query',
            'series': 'series',
            'status': 'status'
        }

        self._query = query
        self._series = series
        self._status = status

    @classmethod
    def from_dict(cls, dikt) -> 'DatapointsResponse':
        """Returns the dict as a model

        :param dikt: A dict.
        :type: dict
        :return: The DatapointsResponse of this DatapointsResponse.  # noqa: E501
        :rtype: DatapointsResponse
        """
        return util.deserialize_model(dikt, cls)

    @property
    def query(self):
        """Gets the query of this DatapointsResponse.


        :return: The query of this DatapointsResponse.
        :rtype: str
        """
        return self._query

    @query.setter
    def query(self, query):
        """Sets the query of this DatapointsResponse.


        :param query: The query of this DatapointsResponse.
        :type query: str
        """

        self._query = query

    @property
    def series(self):
        """Gets the series of this DatapointsResponse.


        :return: The series of this DatapointsResponse.
        :rtype: List[SeriesResponse]
        """
        return self._series

    @series.setter
    def series(self, series):
        """Sets the series of this DatapointsResponse.


        :param series: The series of this DatapointsResponse.
        :type series: List[SeriesResponse]
        """

        self._series = series

    @property
    def status(self):
        """Gets the status of this DatapointsResponse.


        :return: The status of this DatapointsResponse.
        :rtype: str
        """
        return self._status

    @status.setter
    def status(self, status):
        """Sets the status of this DatapointsResponse.


        :param status: The status of this DatapointsResponse.
        :type status: str
        """

        self._status = status
