import fdb
import json
import requests
import os
from datetime import datetime
from tsfdb_server_v1.controllers.queue import Queue

fdb.api_version(620)
ACTIVE_METRIC_MINUTES = int(os.getenv('ACTIVE_METRIC_MINUTES', 60))


class InternalMetrics:
    def __init__(self):
        self.db = fdb.open()
        self.db.options.set_transaction_timeout(10000)
        self.update_status()

    def update_status(self):
        self.status = json.loads(self.db[b'\xff\xff/status/json'])

    def generate_tsfdb_queues_metrics(self):
        try:
            metrics = {}
            available_queues_subspace = fdb.Subspace(('available_queues',))
            count = 0
            for count, (k, _) in enumerate(
                    self.db[available_queues_subspace.range()],
                    1):

                name = available_queues_subspace.unpack(k)[0]
                metric = {f"tsfdb.queue.{name}": Queue(
                    name).count_items(self.db)}
                metrics.update(metric)
            metric = {"tsfdb.queue.count": count}
            metrics.update(metric)
            return metrics
        except fdb.FDBError as err:
            print("ERROR: Could not get queues metrics: %s" %
                  str(err.description, 'utf-8'))
            return {}

    def generate_tsfdb_processes_metrics(self):
        metrics = {
            "tsfdb.cluster.processes":
            len(self.status["cluster"]["processes"]),
            "tsfdb.cluster.degraded_processes":
            self.status["cluster"]["degraded_processes"]
        }
        for _, process in self.status["cluster"]["processes"].items():
            key_prefix = (
                f"tsfdb.{process['machine_id']}.role-"
                f"{process['class_type']}"
            )
            process_metrics = {
                f"{key_prefix}.cpu": process["cpu"]["usage_cores"],
                f"{key_prefix}.disk.busy": process["disk"]["busy"],
                f"{key_prefix}.disk.free":
                process["disk"]["free_bytes"]/1000**3,
                f"{key_prefix}.ram": process["memory"]["used_bytes"]/1000**3
            }
            data_lag = None
            for role in process["roles"]:
                if role and role.get("data_lag", {}).get("seconds"):
                    data_lag = role.get("data_lag").get("seconds")
            if data_lag:
                process_metrics.update({f"{key_prefix}.data_lag": data_lag})
            metrics.update(process_metrics)
        return metrics

    def generate_tsfdb_qos_metrics(self):
        key_prefix = "tsfdb.qos"
        qos = self.status["cluster"]["qos"]
        metrics = {
            f"{key_prefix}.worst_queue_log_size":
            qos["worst_queue_bytes_log_server"]/1000**3,
            f"{key_prefix}.worst_queue_storage_size":
            qos["worst_queue_bytes_storage_server"]/1000**3,
            f"{key_prefix}.worst_data_lag_storage_server":
            qos["worst_data_lag_storage_server"]["seconds"]
        }
        return metrics

    def generate_tsfdb_operations_metrics(self):
        try:
            metrics = {}
            active_metrics = 0
            orgs = fdb.directory.create_or_open(
                self.db, ('monitoring')).list(self.db)
            try:
                for org in orgs:
                    available_metrics = fdb.directory.open(
                        self.db, ('monitoring', org, 'available_metrics'))
                    for _, v in self.db[available_metrics.range()]:
                        values = fdb.tuple.unpack(v)
                        timestamp_metric = 0
                        if len(values) > 1:
                            timestamp_metric = values[1]
                        timestamp_now = datetime.timestamp(datetime.now())
                        if not (abs(timestamp_now - timestamp_metric) / 60 >
                                ACTIVE_METRIC_MINUTES):
                            active_metrics += 1
            except ValueError:
                active_metrics = 0
            operations = self.status["cluster"]["workload"]["operations"]

            key_prefix = "tsfdb.operations"
            metrics = {
                f"{key_prefix}.reads": operations["reads"]["hz"],
                f"{key_prefix}.writes": operations["writes"]["hz"],
                f"{key_prefix}.metrics": active_metrics
            }
            return metrics
        except fdb.FDBError as err:
            print("ERROR: Could not get available metrics: %s" %
                  str(err.description, 'utf-8'))
            return []

    def generate_tsfdb_cluster_data_metrics(self):
        cluster_data = self.status["cluster"]["data"]
        key_prefix = "tsfdb.cluster_data"
        metrics = {
            f"{key_prefix}.total_disk_used":
            cluster_data["total_disk_used_bytes"]/1000**3,
            f"{key_prefix}.total_kv_size":
            cluster_data["total_kv_size_bytes"]/1000**3,
            f"{key_prefix}.least_operating_space_log_server":
            cluster_data["least_operating_space_bytes_log_server"] / 1000**3,
            f"{key_prefix}.least_operating_space_storage_server":
            cluster_data["least_operating_space_bytes_storage_server"] / 1000**3,
            f"{key_prefix}.moving_data.in_flight":
            cluster_data["moving_data"]["in_flight_bytes"] / 1000**3,
            f"{key_prefix}.moving_data.in_queue":
            cluster_data["moving_data"]["in_queue_bytes"] / 1000**3
        }
        return metrics

    def get_all(self):
        metrics = {}
        metrics.update(self.generate_tsfdb_queues_metrics())
        metrics.update(self.generate_tsfdb_processes_metrics())
        metrics.update(self.generate_tsfdb_qos_metrics())
        metrics.update(self.generate_tsfdb_operations_metrics())
        metrics.update(self.generate_tsfdb_cluster_data_metrics())
        return metrics
