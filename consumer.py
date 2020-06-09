import fdb
from time import sleep
from datetime import datetime
from tsfdb_server_v1.controllers.db import open_db, write_in_kv
from tsfdb_server_v1.controllers.queue import Queue
from tsfdb_server_v1.controllers.helpers import error, config


@fdb.transactional
def acquire_queue(tr, available_queues):
    for k, v in tr[available_queues.range()]:
        queue_name = fdb.tuple.unpack(k)[1]
        consumer_lock = None
        if tr[fdb.Subspace(('consumer_lock', queue_name))].present():
            consumer_lock = fdb.tuple.unpack(
                tr[fdb.Subspace(('consumer_lock', queue_name))])[0]
        if consumer_lock is None or \
                int(datetime.now().timestamp()) - \
                consumer_lock > config('ACQUIRE_TIMEOUT'):
            tr[fdb.Subspace(('consumer_lock', queue_name))] = \
                fdb.tuple.pack((int(datetime.now().timestamp()),))
            return queue_name
    return None


def consume_queue(db, acquired_queue):
    queue = Queue(acquired_queue)
    while True:
        try:
            item = queue.pop(db)
            if item:
                org, data = item
                write_in_kv(org, data)
            else:
                sleep(config('CONSUME_TIMEOUT'))
                if queue.delete_if_empty(db):
                    return
        except fdb.FDBError as err:
            if err.code != 1020:
                db.on_error(err.code).wait()
            return
        except ValueError as err:
            error(500, "Garbage data in queue: %s" % queue.name, traceback=err)


def main():
    db = open_db()
    available_queues = fdb.Subspace(('available_queues',))
    while True:
        try:
            acquired_queue = acquire_queue(db, available_queues)
        except fdb.FDBError as err:
            if err.code != 1020:
                db.on_error(err.code).wait()
            acquired_queue = None
        if acquired_queue:
            consume_queue(db, acquired_queue)
        else:
            print("Retrying to acquire a queue in %ds" %
                  config('QUEUE_RETRY_TIMEOUT'))
            sleep(config('QUEUE_RETRY_TIMEOUT'))


if __name__ == "__main__":
    main()
