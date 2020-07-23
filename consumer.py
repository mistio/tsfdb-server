import fdb
import random
from time import sleep
from datetime import datetime
from tsfdb_server_v1.controllers.db import open_db, write_in_kv
from tsfdb_server_v1.controllers.queue import Queue
from tsfdb_server_v1.controllers.helpers import error, config


@fdb.transactional
def acquire_queue(tr, available_queues, queue_names):
    for queue_name in queue_names:
        consumer_lock = None
        queue_names.remove(queue_name)
        if not tr[available_queues.pack((queue_name,))].present():
            continue
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
                item = fdb.tuple.unpack(item)
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
        queue_names = []
        for k, _ in db[available_queues.range()]:
            queue_names.append(fdb.tuple.unpack(k)[1])
        random.shuffle(queue_names)
        while queue_names:
            try:
                acquired_queue = acquire_queue(
                    db, available_queues, queue_names)
            except fdb.FDBError as err:
                if err.code != 1020:
                    db.on_error(err.code).wait()
                acquired_queue = None
            if acquired_queue:
                consume_queue(db, acquired_queue)
            else:
                sleep_time = random.randint(1, config('QUEUE_RETRY_TIMEOUT'))
                print("Retrying to acquire a queue in %ds" %
                      sleep_time)
                sleep(sleep_time)


if __name__ == "__main__":
    main()
