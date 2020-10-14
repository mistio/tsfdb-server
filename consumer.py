import fdb
import random
from time import sleep
from datetime import datetime
from tsfdb_server_v1.controllers.db import DBOperations
from tsfdb_server_v1.controllers.queue import Queue
from tsfdb_server_v1.controllers.helpers import error, config


class Consumer:
    def __init__(self):
        self.db_ops = DBOperations()
        self.available_queues_subspace = fdb.Subspace(('available_queues',))
        self.consumer_lock_subspace = fdb.Subspace(('consumer_lock',))

    @fdb.transactional
    def acquire_queue(self, tr, queue_names):
        for queue_name in queue_names:
            consumer_lock = None
            queue_names.remove(queue_name)
            if not tr[self.available_queues_subspace.pack((queue_name,))].present():
                continue
            if tr[self.consumer_lock_subspace.pack((queue_name,))].present():
                consumer_lock = fdb.tuple.unpack(
                    tr[self.consumer_lock_subspace.pack((queue_name,))])[0]
            if consumer_lock is None or \
                    int(datetime.now().timestamp()) - \
                    consumer_lock > config('ACQUIRE_TIMEOUT'):
                tr[self.consumer_lock_subspace.pack((queue_name,))] = \
                    fdb.tuple.pack((int(datetime.now().timestamp()),))
                return queue_name
        return None


    def consume_queue(self, acquired_queue):
        queue = Queue(acquired_queue)
        while True:
            try:
                item = queue.pop(self.db_ops.db)
                if item:
                    item = fdb.tuple.unpack(item)
                    org, data = item
                    self.db_ops.write_in_kv(org, data)
                else:
                    sleep(config('CONSUME_TIMEOUT'))
                    if queue.delete_if_empty(self.db_ops.db):
                        return
            except fdb.FDBError as err:
                if err.code != 1020:
                    self.db_ops.db.on_error(err.code).wait()
                return
            except ValueError as err:
                error(500, "Garbage data in queue: %s" % queue.name, traceback=err)

    def run(self):
        while True:
            queue_names = []
            for k, _ in self.db_ops.db[self.available_queues_subspace.range()]:
                queue_names.append(fdb.tuple.unpack(k)[1])
            random.shuffle(queue_names)
            while queue_names:
                try:
                    acquired_queue = self.acquire_queue(
                        self.db_ops.db, queue_names)
                except fdb.FDBError as err:
                    if err.code != 1020:
                        self.db_ops.db.on_error(err.code).wait()
                    acquired_queue = None
                if acquired_queue:
                    self.consume_queue(acquired_queue)
                else:
                    sleep_time = random.randint(1, config('QUEUE_RETRY_TIMEOUT'))
                    print("Retrying to acquire a queue in %ds" %
                        sleep_time)
                    sleep(sleep_time)


def main():
    Consumer().run()

if __name__ == "__main__":
    main()
