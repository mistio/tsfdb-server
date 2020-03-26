from time import sleep

from tsfdb_server_v1.controllers.db import open_db, write_in_kv
from tsfdb_server_v1.controllers.queue import Queue, Subspace


def main():
    db = open_db()
    queue = Queue(Subspace(('queue',)))
    while True:
        data = queue.pop(db)
        if data:
            write_in_kv(data)
        else:
            sleep(1)


if __name__ == "__main__":
    main()
