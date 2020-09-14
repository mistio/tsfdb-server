import fdb
import json
import requests
import os
from time import sleep
from datetime import datetime
from tsfdb_server_v1.controllers.queue import Queue

fdb.api_version(620)
TSFDB_URI = os.getenv('TSFDB_URI', "http://tsfdb:8080")
ACTIVE_METRIC_MINUTES = int(os.getenv('ACTIVE_METRIC_MINUTES', 60))


def generate_tsfdb_queues_metrics(db, timestamp):
    try:
        line = "queue,machine_id=tsfdb "
        available_queues_subspace = fdb.Subspace(('available_queues',))
        count = 0
        for count, (k, v) in enumerate(db[available_queues_subspace.range()],
                                       1):
            name = available_queues_subspace.unpack(k)[0]
            line += "%s=%d," % (name, Queue(name).count_items(db))
        line += "count=%d %s" % (count, timestamp)
        return [line]
    except fdb.FDBError as err:
        print("ERROR: Could not get queues metrics: %s" %
              str(err.description, 'utf-8'))
        return []


def generate_tsfdb_processes_metrics(status, timestamp):
    lines = []
    line = ("cluster,machine_id=tsfdb processes=%d,degraded_processes=%d %s" %
            (len(status["cluster"]["processes"]),
             status["cluster"]["degraded_processes"], timestamp))
    lines.append(line)
    for _, process in status["cluster"]["processes"].items():
        line = (("%s,machine_id=tsfdb,role=%s cpu=%f,disk.busy=%f,disk.free=%f"
                 + ",ram=%f") %
                (process["machine_id"], process["class_type"],
                    process["cpu"]["usage_cores"],
                    process["disk"]["busy"],
                    process["disk"]["free_bytes"]/1000**3,
                    process["memory"]["used_bytes"]/1000**3))
        data_lag = None
        for role in process["roles"]:
            if role and role.get("data_lag", {}).get("seconds"):
                data_lag = role.get("data_lag").get("seconds")
        if data_lag:
            line += ",data_lag=%f" % data_lag
        line += " %s" % timestamp
        lines.append(line)
    return lines


def generate_tsfdb_qos_metrics(status, timestamp):
    lines = []
    qos = status["cluster"]["qos"]
    lines.append(("qos,machine_id=tsfdb worst_queue_log_size=%f," +
                  "worst_queue_storage_size=%f,worst_data_lag_storage=%f" +
                  " %s") %
                 (qos["worst_queue_bytes_log_server"]/1000**3,
                  qos["worst_queue_bytes_storage_server"]/1000**3,
                  qos["worst_data_lag_storage_server"]["seconds"],
                  timestamp))
    return lines


def generate_tsfdb_operations_metrics(db, status, timestamp):
    try:
        lines = []
        active_metrics = 0
        active_resources = set()
        orgs = fdb.directory.create_or_open(db, ('monitoring')).list(db)
        try:
            for org in orgs:
                available_metrics = fdb.directory.open(
                    db, ('monitoring', org, 'available_metrics'))
                for k, v in db[available_metrics.range()]:
                    values = fdb.tuple.unpack(v)
                    resource_id, _ = available_metrics.unpack(k)
                    timestamp_metric = 0
                    if len(values) > 1:
                        timestamp_metric = values[1]
                    timestamp_now = datetime.timestamp(datetime.now())
                    if not abs(timestamp_now - timestamp_metric) / 60 > \
                            ACTIVE_METRIC_MINUTES:
                        active_metrics += 1
                        active_resources.add((org, resource_id))
        except ValueError:
            active_metrics = 0
        operations = status["cluster"]["workload"]["operations"]
        lines.append(("operations,machine_id=tsfdb reads=%f,writes=%f," +
                      "metrics=%d,resources=%d %s") %
                     (operations["reads"]["hz"], operations["writes"]["hz"],
                      active_metrics, len(active_resources), timestamp))
        return lines
    except fdb.FDBError as err:
        print("ERROR: Could not get available metrics: %s" %
              str(err.description, 'utf-8'))
        return []


def generate_tsfdb_cluster_data_metrics(status, timestamp):
    lines = []
    cluster_data = status["cluster"]["data"]
    lines.append(("cluster_data,machine_id=tsfdb total_disk_used=%f," +
                  "total_kv_size=%f,least_operating_space_log_server=%f," +
                  "least_operating_space_storage_server=%f," +
                  "moving_data.in_flight=%f," +
                  "moving_data.in_queue=%f %s") %
                 (cluster_data["total_disk_used_bytes"]/1000**3,
                  cluster_data["total_kv_size_bytes"]/1000**3,
                  cluster_data["least_operating_space_bytes_log_server"]
                  / 1000**3,
                  cluster_data["least_operating_space_bytes_storage_server"]
                  / 1000**3,
                  cluster_data["moving_data"]["in_flight_bytes"]
                  / 1000**3,
                  cluster_data["moving_data"]["in_queue_bytes"]
                  / 1000**3,
                  timestamp))
    return lines


def main():
    db = fdb.open()
    db.options.set_transaction_timeout(10000)
    while True:
        lines = []
        dt = datetime.now()
        status = {}
        try:
            status = json.loads(db[b'\xff\xff/status/json'])
        except fdb.FDBError as err:
            print("ERROR: Could not get fdb metrics: %s" %
                  str(err.description, 'utf-8'))
            sleep(5)
            continue
        timestamp = str(int(dt.timestamp())) + 9 * '0'

        lines += generate_tsfdb_operations_metrics(db, status, timestamp)
        lines += generate_tsfdb_processes_metrics(status, timestamp)
        lines += generate_tsfdb_qos_metrics(status, timestamp)
        lines += generate_tsfdb_queues_metrics(db, timestamp)
        lines += generate_tsfdb_cluster_data_metrics(status, timestamp)

        print("\n".join(lines))

        requests.post(
            "%s/v1/datapoints"
            % TSFDB_URI,
            data="\n".join(lines), headers={'x-org-id': 'tsfdb'}
        )
        sleep(5)


if __name__ == "__main__":
    main()
