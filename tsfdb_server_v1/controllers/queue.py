#
# queue.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import fdb
import fdb.tuple
from datetime import datetime
fdb.api_version(620)


class Queue:
    def __init__(self, name, consumer=False, db=None):
        self.queue = fdb.Subspace(('queue', name))
        self.last_push = fdb.Subspace(('last_push', name))
        self.last_pop = fdb.Subspace(('last_pop', name))
        if not consumer:
            print("Creating queue: %s" % (name))
            db[fdb.Subspace(('available_queues', name))] = fdb.tuple.pack((0,))

    @fdb.transactional
    def pop(self, tr):
        item = self.first_item(tr)
        # Update the timestamp in order to indicate
        # that this queue is being served by a consumer
        tr[self.last_pop] = fdb.tuple.pack(
            (int(datetime.now().timestamp()),))
        if item is None:
            return None
        del tr[item.key]
        return fdb.tuple.unpack(item.value)[0]

    @fdb.transactional
    def push(self, tr, value):
        tr[self.last_push] = fdb.tuple.pack(
            (int(datetime.now().timestamp()),))
        tr[self.queue[self.last_index(tr) + 1][os.urandom(20)]] = \
            fdb.tuple.pack((value,))

    @fdb.transactional
    def last_index(self, tr):
        r = self.queue.range()
        for key, _ in tr.snapshot.get_range(r.start, r.stop, limit=1,
                                            reverse=True):
            return self.queue.unpack(key)[0]
        return 0

    @fdb.transactional
    def first_item(self, tr):
        r = self.queue.range()
        for kv in tr.get_range(r.start, r.stop, limit=1):
            return kv

    @fdb.transactional
    def last_push_timestamp(self, tr):
        last_push = 0
        if tr[self.last_push].present():
            last_push = fdb.tuple.unpack(tr[self.last_push])[0]
        return last_push
