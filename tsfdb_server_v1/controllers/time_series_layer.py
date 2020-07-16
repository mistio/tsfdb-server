import asyncio
import fdb
import fdb.tuple
import logging
import re
import struct
from .helpers import metric_to_dict, error, config, div_datapoints, \
    time_range_to_resolution, config
from .tsfdb_tuple import tuple_to_datapoint, time_aggregate_tuple, \
    start_stop_key_tuples
from tsfdb_server_v1.models.error import Error  # noqa: E501

fdb.api_version(620)

log = logging.getLogger(__name__)


class TimeSeriesLayer():
    def __init__(self):
        self.struct_types = (int, float)
        self.limit = config('DATAPOINTS_PER_READ')

    @fdb.transactional
    def find_orgs(self, tr):
        orgs = fdb.directory.create_or_open(
            tr, ('monitoring')).list(tr)
        return orgs

    @fdb.transactional
    def find_metrics(self, tr, org, resource):
        metrics = {}
        available_metrics = fdb.directory.create_or_open(
            tr, ('monitoring', org, 'available_metrics'))
        for k, v in tr.get_range_startswith(available_metrics.pack(
                (resource,))):
            metric = available_metrics.unpack(k)[1]
            value = fdb.tuple.unpack(v)[0]
            metrics.update(metric_to_dict(metric, value))
        return metrics

    @fdb.transactional
    def find_resources(self, tr, org, regex_resources,
                       authorized_resources=None):
        filtered_resources = []
        resources = set(fdb.directory.create_or_open(
            tr, ('monitoring', org)).list(tr))
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
                        metric, start, stop, datapoints_dir=None,
                        available_metrics=None):

        time_range = stop - start
        time_range_in_hours = round(time_range.total_seconds() / 3600, 2)
        stats = (None,)
        datapoints_per_stat = {}
        if time_range_in_hours > config('SECONDS_RANGE'):
            stats = ("count", "sum")

        resolution = time_range_to_resolution(time_range_in_hours)
        if not available_metrics:
            available_metrics = fdb.directory.create_or_open(
                db, ('monitoring', org, 'available_metrics'))
        if not datapoints_dir:
            datapoints_dir = fdb.directory.create_or_open(
                db, ('monitoring', org, resource, resolution))

        for stat in stats:
            tuples = start_stop_key_tuples(
                db, time_range_in_hours,
                resource, metric, start,
                stop, stat, self.limit
            )
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            datapoints_per_stat[stat] = loop.run_until_complete(
                self.__async_find_datapoints_per_stat(db, tuples,
                                                      time_range_in_hours,
                                                      org, resource, metric,
                                                      stat, datapoints_dir,
                                                      available_metrics))

            if isinstance(datapoints_per_stat[stat], Error):
                return datapoints_per_stat[stat]

        if time_range_in_hours > config('SECONDS_RANGE'):
            datapoints = div_datapoints(list(
                datapoints_per_stat["sum"]),
                list(datapoints_per_stat["count"]))
        else:
            datapoints = list(datapoints_per_stat[None])

        return {("%s.%s" % (resource, metric)): datapoints}

    async def __async_find_datapoints_per_stat(self, db, tuples,
                                               time_range_in_hours,
                                               org, resource, metric, stat,
                                               datapoints_dir=None,
                                               available_metrics=None):
        loop = asyncio.get_event_loop()
        data_lists = [
            loop.run_in_executor(None, self.__find_datapoints_per_stat, *
                                 (db, start, stop, time_range_in_hours,
                                  org, resource, metric, stat, datapoints_dir,
                                  available_metrics))
            for start, stop in zip(tuples, tuples[1:])
        ]

        data_lists = await asyncio.gather(*data_lists)
        combined_data_list = []
        for data_list in data_lists:
            combined_data_list += data_list
        return combined_data_list

    @fdb.transactional
    def __find_datapoints_per_stat(self, tr, start, stop, time_range_in_hours,
                                   org, resource, metric, stat,
                                   datapoints_dir=None,
                                   available_metrics=None):

        if not available_metrics:
            available_metrics = fdb.directory.create_or_open(
                tr, ('monitoring', org, 'available_metrics'))
        if not tr[available_metrics.pack(
                (resource, metric))].present():
            error_msg = "Metric type: %s for resource: %s doesn't exist." % (
                metric, resource)
            return error(404, error_msg)
        metric_type_tuple = tr[available_metrics.
                               pack((resource, metric))]
        metric_type = fdb.tuple.unpack(metric_type_tuple)[0]

        datapoints = []
        resolution = time_range_to_resolution(time_range_in_hours)
        if not datapoints_dir:
            datapoints_dir = fdb.directory.create_or_open(
                tr, ('monitoring', org, resource, resolution))
        for k, v in tr.get_range(datapoints_dir.pack(start),
                                 datapoints_dir.pack(stop),
                                 streaming_mode=fdb.StreamingMode.want_all):
            tuple_key = list(fdb.tuple.unpack(k))
            if time_range_in_hours <= config('SECONDS_RANGE'):
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
    def write_datapoint(self, tr, org, resource, key, value,
                        resolution='second', datapoints_dir=None):
        if not datapoints_dir:
            datapoints_dir = fdb.directory.create_or_open(
                tr, ('monitoring', org, resource, resolution))
        if config('CHECK_DUPLICATES'):
            if not tr[datapoints_dir.pack(key)].present():
                tr[datapoints_dir.pack(key)] = fdb.tuple.pack(
                    (value,))
                return True
            saved_value = fdb.tuple.unpack(
                tr[datapoints_dir.pack(key)])[0]
            if saved_value != value:
                log.error("key: %s already exists with a different value" %
                          str(key))
            else:
                log.warning(
                    "key: %s already exists with the same value" % str(key))
            return False

        tr[datapoints_dir.pack(key)] = fdb.tuple.pack(
            (value,))
        return True

    @fdb.transactional
    def write_datapoint_aggregated(self, tr, org, resource,
                                   metric, dt, value, resolution,
                                   datapoints_dir=None):
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
        if not datapoints_dir:
            datapoints_dir = fdb.directory.create_or_open(
                tr, ('monitoring', org, resource, resolution))
        tr.add(datapoints_dir.pack(
            time_aggregate_tuple(metric, "count", dt, resolution)),
            struct.pack('<q', 1))
        tr.add(datapoints_dir.pack(
            time_aggregate_tuple(metric, "sum", dt, resolution)),
            struct.pack('<q', value))
        tr.min(datapoints_dir.pack(
            time_aggregate_tuple(metric, "min", dt, resolution)),
            struct.pack('<q', value))
        tr.max(datapoints_dir.pack(
            time_aggregate_tuple(metric, "max", dt, resolution)),
            struct.pack('<q', value))

    @fdb.transactional
    def add_metric(self, tr, org, metric, metric_type):
        available_metrics = fdb.directory.create_or_open(
            tr, ('monitoring', org, 'available_metrics'))
        if not tr[available_metrics.pack(
                metric)].present():
            tr[available_metrics.pack(
                metric)] = fdb.tuple.pack((metric_type,))

    @fdb.transactional
    def delete_datapoints(self, tr, org, resource,
                          metric, start, stop, resolution):

        resolutions = {
            "second": config('SECONDS_RANGE'),
            "minute": config('MINUTES_RANGE'),
            "hour": config('HOURS_RANGE'),
            "day": config('HOURS_RANGE') + 1
        }

        time_range_in_hours = resolutions.get(
            resolution, config('SECONDS_RANGE'))
        stats = (None,)
        if time_range_in_hours > config('SECONDS_RANGE'):
            stats = ("count", "sum")

        for stat in stats:
            tuples = start_stop_key_tuples(
                tr, time_range_in_hours,
                resource, metric, start,
                stop, stat
            )

            key_timestamp_start, key_timestamp_stop = tuples

            datapoints_dir = fdb.directory.create_or_open(
                tr, ('monitoring', org, resource, resolution))
            tr.clear_range(datapoints_dir.pack(key_timestamp_start),
                           datapoints_dir.pack(key_timestamp_stop))
