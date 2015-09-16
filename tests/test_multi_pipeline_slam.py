# coding: utf-8
# Copyright 2009 Alexandre Fiori
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from twisted.internet import threads
from twisted.internet.defer import inlineCallbacks
from twisted.trial import unittest

import txredisapi as redis

from tests.mixins import REDIS_HOST, REDIS_PORT

import time
import random
import uuid


class TestSimultaneousPipelineAndMulti(unittest.TestCase):
    @inlineCallbacks
    def test_pipeline_and_multi(self):
        db = yield redis.ConnectionPool(
            REDIS_HOST, REDIS_PORT, poolsize=2, reconnect=False)
        for _ in range(1000):
            yield self._run_pipeline(db=db)
            yield self._run_transaction(db=db)
        yield db.disconnect()

    @inlineCallbacks
    def _run_pipeline(self, db):
        yield threads.deferToThread(
            lambda: time.sleep(random.uniform(0, 0.0001)))
        pipeline = yield db.pipeline()

        c_id = str(uuid.uuid4()).replace('-', '')

        pipeline.sadd('fred', c_id)
        pipeline.expire("fred:%s" % c_id, 60 * 60 * 24)

        yield pipeline.execute_pipeline()

    @inlineCallbacks
    def _run_transaction(self, db):
        yield threads.deferToThread(
            lambda: time.sleep(random.uniform(0, 0.0001)))
        test1 = {u"foo1": u"bar1", u"something": u"bob"}
        test2 = {u"foo2": u"bar2", u"something": u"else"}

        t = yield db.watch('txredisapi:nmb:test1')
        yield t.multi()
        yield t.hmset("txredisapi:nmb:test1", test1)
        yield t.hgetall("txredisapi:nmb:test1")
        yield t.hmset("txredisapi:nmb:test2", test2)
        yield t.hgetall("txredisapi:nmb:test2")
        r = yield t.commit()
        self.assertEqual(r[0], "OK")
        self.assertEqual(sorted(r[1].keys()), sorted(test1.keys()))
        self.assertEqual(sorted(r[1].values()), sorted(test1.values()))
        self.assertEqual(r[2], "OK")
        self.assertEqual(sorted(r[3].keys()), sorted(test2.keys()))
        self.assertEqual(sorted(r[3].values()), sorted(test2.values()))
        self.assertFalse(t.pipelining)
