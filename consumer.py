import fdb
from time import sleep
from datetime import datetime
from tsfdb_server_v1.controllers.db import open_db, write_in_kv
from tsfdb_server_v1.controllers.queue import Queue


@fdb.transactional
def acquire_queue(tr, available_queues):
    try:
        for k, v in tr[available_queues.range()]:
            queue_name = fdb.tuple.unpack(k)[1]
            last_pop = None
            if tr[fdb.Subspace(('last_pop', queue_name))].present():
                last_pop = fdb.tuple.unpack(
                    tr[fdb.Subspace(('last_pop', queue_name))])[0]
            print(last_pop)
            if last_pop is None or \
                    int(datetime.now().timestamp()) - last_pop > 30:
                tr[fdb.Subspace(('last_pop', queue_name))] = fdb.tuple.pack(
                    (int(datetime.now().timestamp()),))
                return queue_name
        return None
    except fdb.FDBError as err:
        if err.code != 1020:
            tr.on_error(err.code).wait()
        return None


def serve_queue(db, acquired_queue):
    queue = Queue(acquired_queue, consumer=True)
    while True:
        try:
            data = queue.pop(db)
            if data:
                write_in_kv(data)
            else:
                sleep(1)
                if int(datetime.now().timestamp()) - queue.last_push_timestamp(db) > 30:
                    delete_queue(db, acquired_queue)
                    print("Deleted queue: %s" % (acquired_queue))
                    return
        except fdb.FDBError as err:
            print(err.description)


@fdb.transactional
def delete_queue(tr, queue_to_delete):
    del tr[fdb.Subspace(('queue', queue_to_delete))]
    del tr[fdb.Subspace(('last_push', queue_to_delete))]
    del tr[fdb.Subspace(('last_pop', queue_to_delete))]
    del tr[fdb.Subspace(('available_queues', queue_to_delete))]


def main():
    db = open_db()
    available_queues = fdb.Subspace(('available_queues',))
    while True:
        acquired_queue = acquire_queue(db, available_queues)
        if acquired_queue:
            serve_queue(db, acquired_queue)
        else:
            print("Retrying to acquire a queue in 30s")
            sleep(30)


if __name__ == "__main__":
    main()
