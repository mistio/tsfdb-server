import fdb
import json
import requests
import os
from time import sleep
from datetime import datetime

fdb.api_version(620)
TSFDB_URI = os.getenv('TSFDB_URI', "http://tsfdb:8080")


def main():
    db = fdb.open()
    while True:
        lines = []
        dt = datetime.now()
        status = json.loads(db[b'\xff\xff/status/json'])
        timestamp = str(int(dt.timestamp())) + 9 * '0'
        for _, process in status["cluster"]["processes"].items():
            lines.append(("%s,machine_id=tsfdb,role=%s cpu=%f,disk=%f,ram=%f" +
                          " %s") %
                         (process["machine_id"], process["class_type"],
                          process["cpu"]["usage_cores"],
                          process["disk"]["busy"],
                          process["memory"]["used_bytes"]/1000**3, timestamp))
        qos = status["cluster"]["qos"]
        lines.append(("qos,machine_id=tsfdb worst_queue_log_size=%f," +
                      "worst_queue_storage_size=%f,worst_data_lag_storage=%f" +
                      " %s") %
                     (qos["worst_queue_bytes_log_server"]/1000**3,
                      qos["worst_queue_bytes_storage_server"]/1000**3,
                      qos["worst_data_lag_storage_server"]["seconds"],
                      timestamp))
        operations = status["cluster"]["workload"]["operations"]
        lines.append("operations,machine_id=tsfdb reads=%f,writes=%f %s" %
                     (operations["reads"]["hz"], operations["writes"]["hz"],
                      timestamp))
        print("\n".join(lines))

        requests.post(
            "%s/v1/datapoints"
            % TSFDB_URI,
            data="\n".join(lines)
        )
        sleep(5)


if __name__ == "__main__":
    main()
