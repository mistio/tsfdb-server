import fdb
from time import sleep
from datetime import datetime
from tsfdb_server_v1.controllers.db import open_db, write_in_kv
from tsfdb_server_v1.controllers.queue import Queue

ACQUIRE_TIMEOUT = 30
CONSUME_TIMEOUT = 1
RETRY_TIMEOUT = 5


@fdb.transactional
def acquire_queue(tr, available_queues):
    try:
        for k, v in tr[available_queues.range()]:
            queue_name = fdb.tuple.unpack(k)[1]
            consumer_lock = None
            if tr[fdb.Subspace(('consumer_lock', queue_name))].present():
                consumer_lock = fdb.tuple.unpack(
                    tr[fdb.Subspace(('consumer_lock', queue_name))])[0]
            if consumer_lock is None or \
                    int(datetime.now().timestamp()) - \
                    consumer_lock > ACQUIRE_TIMEOUT:
                tr[fdb.Subspace(('consumer_lock', queue_name))] = \
                    fdb.tuple.pack((int(datetime.now().timestamp()),))
                return queue_name
        return None
    except fdb.FDBError as err:
        if err.code != 1020:
            tr.on_error(err.code).wait()
        return None


def consume_queue(db, acquired_queue):
    queue = Queue(acquired_queue)
    while True:
        try:
            data = queue.pop(db)
            if data:
                write_in_kv(data)
            else:
                sleep(CONSUME_TIMEOUT)
                if queue.delete_if_empty(db):
                    return
        except fdb.FDBError as err:
            if err.code != 1020:
                db.on_error(err.code).wait()


def main():
    db = open_db()
    available_queues = fdb.Subspace(('available_queues',))
    while True:
        acquired_queue = acquire_queue(db, available_queues)
        if acquired_queue:
            consume_queue(db, acquired_queue)
        else:
            print("Retrying to acquire a queue in %ds" %
                  RETRY_TIMEOUT)
            sleep(RETRY_TIMEOUT)


if __name__ == "__main__":
    main()
