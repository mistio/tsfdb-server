import base64
import fdb
import json
import os
import re
from time import sleep
from datetime import datetime
from tsfdb_server_v1.controllers.time_series_layer import TimeSeriesLayer
from tsfdb_server_v1.controllers.helpers import parse_start_stop_params

fdb.api_version(620)
time_series = TimeSeriesLayer()
db = fdb.open()


def main():
    raw_config = os.getenv('CONFIG', None)
    configs = []
    if raw_config:
        configs = json.loads(base64.b64decode(raw_config))
    else:
        print("No config file found")

    while True:
        all_orgs = time_series.find_orgs(db)
        all_resources = {}
        all_metrics = {}

        for org in all_orgs:
            resources = time_series.find_resources(db, org, "*")
            all_resources[org] = resources
            for resource in resources:
                metrics = time_series.find_metrics(
                    db, org, resource)
                all_metrics[resource] = set(metrics)

        for config in configs:
            regex = next(iter(config))
            retentions = config[regex]
            regex_org, regex_resource, regex_metric = regex.split(",")

            config_orgs = []
            config_resources = {}
            config_metrics = {}

            config_orgs = filter_items(regex_org, all_orgs)

            for org in config_orgs:
                config_resources[org] = filter_items(
                    regex_resource, all_resources[org])
                for resource in config_resources[org]:
                    config_metrics[resource] = filter_items(
                        regex_metric, all_metrics[resource])
                    all_metrics[resource] -= set(config_metrics[resource])

            apply_retention_policy(db, retentions, config_orgs,
                                   config_resources, config_metrics)
        sleep(10)


def apply_retention_policy(db, retentions, orgs, resources, metrics):
    for resolution, retention_period in retentions.items():
        stop = "-" + retention_period
        _, stop = parse_start_stop_params("", stop)
        start = datetime.min
        for org in orgs:
            for resource in resources[org]:
                for metric in metrics[resource]:
                    time_series.delete_datapoints(
                        db, org, resource, metric, start, stop, resolution)


def filter_items(regex, all_items):
    if regex == "*":
        return list(all_items)
    return [item for item in all_items if re.match("^%s$" % regex, item)]


if __name__ == "__main__":
    main()
