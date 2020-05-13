import fdb
import os
from time import sleep

from tsfdb_server_v1.controllers.db import open_db, write_in_kv
from tsfdb_server_v1.controllers.queue import Queue


def main():
    db = open_db()
    queue = Queue(fdb.Subspace(('queue', os.uname()[1])))
    while True:
        try:
            data = queue.pop(db)
            if data:
                write_in_kv(data)
            else:
                sleep(1)
        except fdb.FDBError as err:
            print(err.description)


if __name__ == "__main__":
    main()
