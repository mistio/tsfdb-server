import asyncio
import fdb
import fdb.tuple
import re
import logging
import traceback
from .tsfdb_tuple import key_tuple_second, delta_dt
from .helpers import error, parse_start_stop_params, \
    generate_metric, profile, is_regex, config, get_queue_id, \
    time_range_to_resolution, get_fallback_resolution, filter_artifacts
from .queue import Queue
from line_protocol_parser import parse_line
from datetime import datetime
from tsfdb_server_v1.models.error import Error  # noqa: E501
from .time_series_layer import TimeSeriesLayer

fdb.api_version(620)


class DBOperations:
    def __init__(self, series_type="monitoring", resolution=""):
        self.log = logging.getLogger(__name__)
        self.resolutions = ("minute", "hour", "day")
        self.db = self.open_db()
        self.time_series = TimeSeriesLayer(series_type)
        self.resolution = resolution

    @staticmethod
    def open_db():
        db = fdb.open()
        db.options.set_transaction_retry_limit(
            config('TRANSACTION_RETRY_LIMIT'))
        db.options.set_transaction_timeout(config('TRANSACTION_TIMEOUT'))
        return db

    def find_metrics(self, org, resource):
        try:
            return self.time_series.find_metrics(self.db, org, resource)
        except fdb.FDBError as err:
            error_msg = (
                "%s on find_metrics(resource) with resource_id: %s" % (
                    str(err.description, 'utf-8'),
                    resource))
            return error(503, error_msg, traceback=traceback.format_exc(),
                         request=resource)

    def find_resources(self, org, regex_resources, authorized_resources=None):
        try:
            return self.time_series.find_resources(self.db, org,
                                                   regex_resources,
                                                   authorized_resources)
        except fdb.FDBError as err:
            error_msg = (
                ("%s on find_resources(regex_resources)" +
                 " with regex_resources: %s")
                % (
                    str(err.description, 'utf-8'),
                    regex_resources)
            )
            return error(503, error_msg, traceback=traceback.format_exc(),
                         request=regex_resources)

    async def async_find_datapoints(self, org, resource, start, stop, metrics):
        metrics_data = []
        try:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            data = {}
            start, stop = parse_start_stop_params(start, stop)

            time_range = stop - start
            time_range_in_hours = round(time_range.total_seconds() / 3600, 2)

            resolution = self.resolution
            fallback_resolution = None
            if not resolution:
                resolution = time_range_to_resolution(time_range_in_hours)
                fallback_resolution = get_fallback_resolution(resolution)
            available_metrics = fdb.directory.create_or_open(
                self.db, (self.time_series.series_type, org,
                          'available_metrics'))
            datapoints_dir = fdb.directory.create_or_open(
                self.db, (self.time_series.series_type, org, resource,
                          resolution))
            if fallback_resolution:
                datapoints_fallback_dir = fdb.directory.create_or_open(
                    self.db, (self.time_series.series_type, org, resource,
                              fallback_resolution))

            metrics_data = [
                loop.run_in_executor(None, self.time_series.find_datapoints,
                                     *(self.db, org, resource, metric, start,
                                       stop, datapoints_dir, available_metrics,
                                       resolution))
                for metric in metrics
            ]

            metrics_data = await asyncio.gather(*metrics_data,
                                                return_exceptions=True)
            exceptions = 0
            last_exception = None
            last_error = None
            metrics_data_fallback = []
            metrics_served = []
            for metric_data in metrics_data:
                if isinstance(metric_data, Error):
                    last_error = metric_data
                elif isinstance(metric_data, Exception):
                    exceptions += 1
                    last_exception = metric_data
                elif metric_data:
                    if fallback_resolution:
                        # In case we have data from the appropriate resolution,
                        # according to our config, we try to fill the gaps if
                        # any, from the start of the asked time range till the
                        # oldest datapoint e.g. [start, oldest datapoint, stop]
                        #  -> [start, oldest datapoint]
                        stop_fallback = stop
                        key = next(iter(metric_data))
                        if metric_data.get(key):
                            first_timestamp = metric_data.get(
                                key)[0][1]
                            stop_fallback = datetime.fromtimestamp(
                                first_timestamp) - delta_dt(
                                    fallback_resolution)
                        async_func_call = (None,
                                           self.time_series.find_datapoints,
                                           *(self.db, org, resource,
                                             key.split(".", 1)[1],
                                             start, stop_fallback,
                                             datapoints_fallback_dir,
                                             available_metrics,
                                             fallback_resolution))
                        metrics_data_fallback.append(async_func_call)
                        metric = next(iter(metric_data)).split(".", 1)[1]
                        metrics_served.append(metric)
                    data.update(metric_data)

            if fallback_resolution:
                metrics_not_served = set(metrics) - set(metrics_served)

                for metric in metrics_not_served:
                    # In case we have metrics that didn't have any datapoints,
                    # from the appropriate resolution, we try to get the asked
                    # time range in a lower resolution instead.
                    async_func_call = (None, self.time_series.find_datapoints,
                                       *(self.db, org, resource,
                                         metric,
                                         start, stop,
                                         datapoints_fallback_dir,
                                         available_metrics,
                                         fallback_resolution))
                    metrics_data_fallback.append(async_func_call)

                metrics_data_fallback = await asyncio.gather(
                    *(loop.run_in_executor(*metric_data_fallback)
                      for metric_data_fallback in metrics_data_fallback),
                    return_exceptions=True)

                for metric_data_fallback in metrics_data_fallback:
                    if isinstance(metric_data_fallback, Error):
                        last_error = metric_data_fallback
                        print(last_error)
                    elif isinstance(metric_data_fallback, Exception):
                        exceptions += 1
                        last_exception = metric_data_fallback
                        print(last_exception)
                    elif metric_data_fallback:
                        key = next(iter(metric_data_fallback))
                        if metric_data_fallback.get(key):
                            if data.get(key):
                                data[key] = \
                                    filter_artifacts(start, stop,
                                                     metric_data_fallback.get(
                                                         key)) + \
                                    data[key]
                            else:
                                data[key] = metric_data_fallback.get(key)

            if last_error:
                if not data:
                    return last_error
            if last_exception:
                if not data:
                    raise last_exception
                error(
                    500,
                    "Could not fetch %d/%d metrics from resource: %s"
                    % (exceptions, len(metrics), resource),
                    traceback=str(last_exception))
            return data
        except fdb.FDBError as err:
            error_msg = (
                (("%s Could not fetch any of the" +
                  " %d metrics from resource: %s")) % (
                    str(err.description, 'utf-8'), len(metrics), resource)
            )
            return error(503, error_msg, traceback=traceback.format_exc(),
                         request=str((resource, start, stop, metrics)))

    @fdb.transactional
    def write_lines(self, tr, org, lines):
        metrics = {}
        datapoints_dir = {}
        for line in lines:
            dict_line = parse_line(line)
            machine = dict_line["tags"]["machine_id"]
            metric = generate_metric(
                dict_line["tags"], dict_line["measurement"])
            dt = datetime.fromtimestamp(int(str(dict_line["time"])[:10]))
            if self.time_series.series_type == 'metering':
                dt = datetime.now()
            for field, value in dict_line["fields"].items():
                machine_metric = "%s.%s" % (metric, field)
                if not datapoints_dir.get("second"):
                    datapoints_dir["second"] = fdb.directory.create_or_open(
                        tr, (self.time_series.series_type, org, machine,
                             'second'))
                if self.time_series.write_datapoint(
                        tr, org, machine,
                        key_tuple_second(
                            dt, machine_metric),
                        value,
                        datapoints_dir=datapoints_dir['second']):
                    if not metrics.get(machine):
                        metrics[machine] = set()
                    metrics[machine].add(
                        (machine_metric, type(value).__name__))
                    for resolution in self.resolutions:
                        if not datapoints_dir.get(resolution):
                            datapoints_dir[resolution] = \
                                fdb.directory.create_or_open(
                                tr, (self.time_series.series_type, org,
                                     machine, resolution))
                        self.time_series.write_datapoint_aggregated(
                            tr, org, machine, machine_metric,
                            dt, value, resolution,
                            datapoints_dir=datapoints_dir[resolution])
        return metrics

    @profile
    def write_in_queue(self, org, data):
        try:
            if not data:
                return
            db = self.open_db()
            queue = Queue(get_queue_id(data))
            queue.push(db, (org, data))
            print("Pushed %d bytes" % len(data.encode('utf-8')))
        except fdb.FDBError as err:
            error_msg = ("%s on write_in_queue(data)" % (
                str(err.description, 'utf-8')))
            return error(503, error_msg, traceback=traceback.format_exc(),
                         request=str(data))

    def write_in_kv_base(self, org, data):
        try:
            if not data:
                return

            # Create a list of lines
            data = data.split('\n')
            # Get rid of all empty lines
            data = [line for line in data if line != ""]

            metrics = set()
            total_datapoints = 0
            machine = ""
            for line in data:
                dict_line = parse_line(line)
                machine = dict_line["tags"]["machine_id"]
                total_datapoints += len(dict_line["fields"].items())
                metric = generate_metric(
                    dict_line["tags"], dict_line["measurement"])
                for field, _ in dict_line["fields"].items():
                    metrics.add(machine + "-" + metric + "-" + field)

            self.log.warning((
                "Request for resource: %s, number of metrics: %d," +
                " number of datapoints: %d") % (
                machine, len(metrics), total_datapoints))

            metrics = self.write_lines(self.db, org, data)
            self.update_metrics(self.db, org, metrics)
        except fdb.FDBError as err:
            error_msg = ("%s on write_in_kv(data) with resource_id: %s" % (
                str(err.description, 'utf-8'),
                parse_line(data[0])["tags"]["machine_id"]))
            return error(503, error_msg, traceback=traceback.format_exc(),
                         request=str(data))

    @profile
    def write_in_kv(self, org, data):
        self.write_in_kv_base(org, data)

    @fdb.transactional
    def update_metrics(self, tr, org, new_metrics):
        for machine, metrics in new_metrics.items():
            current_metrics = self.time_series.find_metrics(
                tr, org, machine)
            timestamp_now = datetime.timestamp(datetime.now())
            current_metrics = {
                m for m in current_metrics.keys(
                ) if not abs(timestamp_now - current_metrics.get(m, {}).get(
                    "last_updated", 0)) / 60 >
                config('ACTIVE_METRIC_MINUTES') / 2
            }
            for metric, metric_type in metrics:
                if not (metric in current_metrics):
                    self.time_series.add_metric(tr, org,
                                                (machine, metric),
                                                metric_type)

    async def async_fetch_list(self, org, multiple_resources_and_metrics,
                               start="", stop="", authorized_resources=None):
        data = {}
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        data_list = [
            loop.run_in_executor(None, self.fetch_item, *
                                 (org, resources_and_metrics, start, stop,
                                  authorized_resources))
            for resources_and_metrics in multiple_resources_and_metrics
        ]

        data_list = await asyncio.gather(*data_list)

        for data_item in data_list:
            if isinstance(data_item, Error):
                return data_item
            data.update(data_item)

        return data

    def fetch_item(self, org, resources_and_metrics, start="", stop="",
                   authorized_resources=None):
        resources, metrics = resources_and_metrics.split(".", 1)
        if is_regex(resources):
            regex_resources = resources
            resources = self.find_resources(
                org, regex_resources, authorized_resources)
            if isinstance(resources, Error):
                return resources
        else:
            resources = [resources]

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        data = loop.run_until_complete(
            self.async_fetch_item(org, resources, start, stop, metrics))
        loop.close()
        return data

    async def async_fetch_item(self, org, resources, start, stop, metrics):
        data = {}
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        data_list = [
            loop.run_in_executor(None, self.find_datapoints_per_resource, *
                                 (org, resource, start, stop, metrics))
            for resource in resources
        ]

        data_list = await asyncio.gather(*data_list)
        last_error = None
        for data_item in data_list:
            if isinstance(data_item, Error):
                if data_item.code // 100 != 4:
                    return data_item
                last_error = data_item
            else:
                data.update(data_item)

        if data == {} and last_error:
            return last_error
        return data

    def find_datapoints_per_resource(self, org, resource, start, stop,
                                     metrics):
        data = {}
        if is_regex(metrics):
            regex_metric = metrics
            metrics = []
            all_metrics = self.time_series.find_metrics(self.db, org, resource)
            if isinstance(all_metrics, Error):
                return all_metrics
            if regex_metric == "*":
                metrics = [candidate for candidate in all_metrics]
            else:
                for candidate in all_metrics:
                    if re.match("^%s$" % regex_metric, candidate):
                        metrics.append(candidate)
            if len(metrics) == 0:
                error_msg = (
                    "No metrics for regex: \"%s\" where found" % regex_metric
                )
                return error(400, error_msg, self.log)
        else:
            metrics = [metrics]
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        current_data = loop.run_until_complete(
            self.async_find_datapoints(org, resource, start, stop, metrics))
        loop.close()
        if isinstance(current_data, Error):
            return current_data
        data.update(current_data)
        return data
