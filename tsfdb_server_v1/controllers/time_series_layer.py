import fdb
import fdb.tuple
import logging
import re
import struct
from datetime import datetime
from .helpers import metric_to_dict, error, config, div_datapoints, \
    time_range_to_resolution, config
from .tsfdb_tuple import tuple_to_datapoint, time_aggregate_tuple, \
    start_stop_key_tuples
from tsfdb_server_v1.models.error import Error  # noqa: E501

fdb.api_version(620)

log = logging.getLogger(__name__)


class TimeSeriesLayer():
    def __init__(self):
        self.dir_monitoring = None
        self.dir_orgs = {}
        self.dir_resources = {}
        self.dir_resources_resolutions = {}
        self.dir_available_metrics = {}
        self.struct_types = (int, float)

    def __set_monitoring_dir(self, db):
        if not self.dir_monitoring:
            self.dir_monitoring = fdb.directory.create_or_open(
                db, ('monitoring',))

    def __get_org_dir(self, db, org):
        self.__set_monitoring_dir(db)
        if not self.dir_orgs.get(org):
            self.dir_orgs[org] = self.dir_monitoring.create_or_open(
                db, (org,))
        return self.dir_orgs[org]

    def __get_resource_dir(self, db, org, resource):
        if not self.dir_resources.get(org, {}).get(resource):
            if not self.dir_resources.get(org):
                self.dir_resources[org] = {}
            self.dir_resources[org][resource] = self.__get_org_dir(
                db, org).create_or_open(db, (resource,))
        return self.dir_resources[org][resource]

    def __get_available_metrics_dir(self, db, org):
        if not self.dir_available_metrics.get(org):
            self.dir_available_metrics[org] = self.__get_org_dir(
                db, org).create_or_open(db, ('available_metrics',))
        return self.dir_available_metrics[org]

    def __get_resource_resolution_dir(self, db, org, resource, resolution,
                                      metric, bucket=None):
        if not self.dir_resources_resolutions.get(org, {}).get(
                resource, {}).get(resolution):
            if not self.dir_resources_resolutions.get(org):
                self.dir_resources_resolutions[org] = {}
            if not self.dir_resources_resolutions[org].get(resource):
                self.dir_resources_resolutions[org][resource] = {}
            if not bucket:
                if not self.dir_resources_resolutions[org][resource].get(resolution):
                    self.dir_resources_resolutions[org][resource][resolution] = {
                    }
                if not self.dir_resources_resolutions[org][resource][resolution].get(metric):
                    self.dir_resources_resolutions[org][resource][resolution][metric] = \
                        self.__get_resource_dir(db, org, resource).create_or_open(
                        db, (resolution, metric))
            else:
                self.dir_resources_resolutions[org][resource][resolution] = {
                }
        if bucket:
            if not self.dir_resources_resolutions[org][resource][resolution].get(bucket):
                self.dir_resources_resolutions[org][resource][resolution][bucket] = {
                }
            if not self.dir_resources_resolutions[org][resource][resolution][bucket].get(metric):
                self.dir_resources_resolutions[org][resource][resolution][bucket][metric] = \
                    self.__get_resource_dir(db, org, resource).create_or_open(
                    db, (resolution, bucket, metric))
            return self.dir_resources_resolutions[org][resource][resolution][bucket][metric]
        else:
            if not self.dir_resources_resolutions[org][resource][resolution].get(metric):
                self.dir_resources_resolutions[org][resource][resolution][metric] = \
                    self.__get_resource_dir(db, org, resource).create_or_open(
                    db, (resolution, metric))

        return self.dir_resources_resolutions[org][resource][resolution][metric]

    @fdb.transactional
    def find_metrics(self, tr, org, resource):
        metrics = {}
        for k, v in tr.get_range_startswith(self.__get_available_metrics_dir(
                tr, org).pack((resource,))):
            metric = self.__get_available_metrics_dir(tr, org).unpack(k)[1]
            value = fdb.tuple.unpack(v)[0]
            metrics.update(metric_to_dict(metric, value))
        return metrics

    @fdb.transactional
    def find_resources(self, tr, org, regex_resources,
                       authorized_resources=None):
        filtered_resources = []
        resources = set(self.__get_org_dir(tr, org).list(tr))
        # Remove reserved directory for metrics
        resources.remove('available_metrics')
        # Use only authorized resources
        if authorized_resources:
            authorized_resources = set(authorized_resources)
            resources = resources.union(authorized_resources)
        resources = list(resources)

        if regex_resources == "*":
            filtered_resources = resources
        else:
            for resource in resources:
                if re.match("^%s$" % regex_resources, resource):
                    filtered_resources.append(resource)
        return filtered_resources

    def find_datapoints(self, db, org, resource,
                        metric, start, stop):

        time_range = stop - start
        time_range_in_hours = round(time_range.total_seconds() / 3600, 2)
        stats = (None,)
        buckets = (None,)
        datapoints_per_stat = {}
        if time_range_in_hours > config('SECONDS_RANGE'):
            stats = ("count", "sum")
        else:
            buckets = ['bucket%d' % i for i in range(config('BUCKETS'))]

        for stat in stats:
            tuples = start_stop_key_tuples(
                db, time_range_in_hours,
                resource, start,
                stop, stat
            )

            key_timestamp_start, key_timestamp_stop = tuples

            datapoints_per_stat[stat] = []

            for bucket in buckets:
                result = self.__find_datapoints_per_stat(
                    db, key_timestamp_start, key_timestamp_stop,
                    time_range_in_hours, org, resource, metric, stat, bucket=bucket)
                if isinstance(result, Error):
                    return result
                datapoints_per_stat[stat] += result
        if time_range_in_hours > 1:
            datapoints = div_datapoints(
                datapoints_per_stat["sum"],
                datapoints_per_stat["count"])
        else:
            datapoints = sorted(datapoints_per_stat[None], key=lambda x: x[1])

        return {("%s.%s" % (resource, metric)): datapoints}

    @fdb.transactional
    def __find_datapoints_per_stat(self, tr, start, stop, time_range_in_hours,
                                   org, resource, metric, stat, bucket=None):

        if not tr[self.__get_available_metrics_dir(tr, org).pack(
                (resource, metric))].present():
            error_msg = "Metric type: %s for resource: %s doesn't exist." % (
                resource, metric)
            return error(404, error_msg)
        metric_type_tuple = tr[self.__get_available_metrics_dir(tr, org).
                               pack((resource, metric))]
        metric_type = fdb.tuple.unpack(metric_type_tuple)[0]

        datapoints = []
        resolution = time_range_to_resolution(time_range_in_hours)
        for k, v in tr[self.__get_resource_resolution_dir(
                tr, org, resource, resolution, metric, bucket=bucket).pack(start):
                self.__get_resource_resolution_dir(tr, org, resource,
                                                   resolution, metric, bucket=bucket).pack(stop)]:

            tuple_key = list(fdb.tuple.unpack(k))
            if time_range_in_hours <= 1:
                tuple_value = list(fdb.tuple.unpack(v))
            else:
                tuple_value = v

            datapoints.append(
                tuple_to_datapoint(
                    time_range_in_hours, tuple_value, tuple_key, metric_type,
                    stat
                )
            )
        return datapoints

    @fdb.transactional
    def write_datapoint(self, tr, org, resource, key, metric, value,
                        resolution='second'):
        """if config('CHECK_DUPLICATES'):
            if not tr[self.__get_resource_resolution_dir(
                    tr, org, resource, resolution).pack(key)].present():
                tr[self.__get_resource_resolution_dir(
                    tr, org, resource, resolution).pack(key)] = fdb.tuple.pack(
                        (value,))
                return True
            saved_value = fdb.tuple.unpack(
                tr[self.__get_resource_resolution_dir(
                    tr, org, resource, resolution).pack(key)])[0]
            if saved_value != value:
                log.error("key: %s already exists with a different value" %
                          str(key))
            else:
                log.warning(
                    "key: %s already exists with the same value" % str(key))
            return False"""

        bucket = 'bucket%d' % (int(datetime.timestamp(
            datetime.now()) / 60) % config('BUCKETS'))

        tr[self.__get_resource_resolution_dir(
            tr, org, resource, resolution, metric, bucket=bucket).pack(key)] = \
            fdb.tuple.pack((value,))
        return True

    @fdb.transactional
    def write_datapoint_aggregated(self, tr, org, resource,
                                   metric, dt, value, resolution):
        if type(value) not in self.struct_types:
            log.warning("Unsupported aggregation value type: %s" %
                        str(type(value)))
            return
        if type(value) is float:
            value *= 1000
            value = int(value)
        if not config('AGGREGATE_%s' % resolution.upper()):
            # log something
            return
        tr.add(self.__get_resource_resolution_dir(
            tr, org, resource, resolution, metric).pack(
            time_aggregate_tuple("count", dt, resolution)),
            struct.pack('<q', 1))
        tr.add(self.__get_resource_resolution_dir(
            tr, org, resource, resolution, metric).pack(
            time_aggregate_tuple("sum", dt, resolution)),
            struct.pack('<q', value))
        tr.min(self.__get_resource_resolution_dir(
            tr, org, resource, resolution, metric).pack(
            time_aggregate_tuple("min", dt, resolution)),
            struct.pack('<q', value))
        tr.max(self.__get_resource_resolution_dir(
            tr, org, resource, resolution, metric).pack(
            time_aggregate_tuple("max", dt, resolution)),
            struct.pack('<q', value))

    @fdb.transactional
    def add_metric(self, tr, org, metric, metric_type):
        if not tr[self.__get_available_metrics_dir(tr, org).pack(
                metric)].present():
            tr[self.__get_available_metrics_dir(tr, org).pack(
                metric)] = fdb.tuple.pack((metric_type,))