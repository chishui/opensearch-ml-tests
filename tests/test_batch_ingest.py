import os
import json
import uuid
import time
import random
import unittest
import itertools
from basic import client
from basic.pipeline import Pipeline
from basic.ingest import Ingest
from basic.doc import Doc
from basic.cluster import Cluster
from basic.util import parser


class BatchIngest(unittest.TestCase):
    maxDiff = None
    def __init__(self, *args, **kwargs):
        super(BatchIngest, self).__init__(*args, **kwargs)
        self.pipeline = None
        self.index = None
        self.cluster = Cluster()

    def setUp(self):
        print("setup")

    def tearDown(self):
        print("start clean up...")
        if self.pipeline != None:
            self.pipeline.delete()
        if self.index != None:
            self.index.delete()

        self.pipeline = None
        self.index = None

    def test_one_success_one_fail(self):
        index = str(uuid.uuid4())
        pipeline_name = str(uuid.uuid4())
        # prepare
        self._prepare_pipeline(pipeline_name)
        # ingest and verify
        self._ingest_doc_and_verify(index, ["success", "fail"], batch_size=2)


    def test_one_success_one_drop(self):
        index = str(uuid.uuid4())
        pipeline_name = str(uuid.uuid4())
        # prepare
        self._prepare_pipeline(pipeline_name)
        # ingest and verify
        self._ingest_doc_and_verify(index, ["success", "drop"], batch_size=2)

    def test_one_fail_one_drop(self):
        index = str(uuid.uuid4())
        pipeline_name = str(uuid.uuid4())
        # prepare
        self._prepare_pipeline(pipeline_name)
        # ingest and verify
        self._ingest_doc_and_verify(index, ["fail", "drop"], batch_size=2)

    def test_random(self):
        index = str(uuid.uuid4())
        pipeline_name = str(uuid.uuid4())
        # prepare
        self._prepare_pipeline(pipeline_name)
        # ingest and verify
        choices = list(range(1, 20))
        success = ["success" for i in range(random.choice(choices))]
        fail = ["fail" for i in range(random.choice(choices))]
        drop = ["drop" for i in range(random.choice(choices))]
        plan = success + fail + drop
        random.shuffle(plan)
        self._ingest_doc_and_verify(index, plan, batch_size=random.choice(choices))

    def _ingest_doc_and_verify(self, index, plan, batch_size):
        # index through bulk
        ingest = Ingest(index)
        docs = []
        for i in range(len(plan)):
            p = plan[i]
            if p == "success":
                p = "success_" + str(i+1)
            docs.append({"id": "batch_"+str(i+1), "text": p})
        response = ingest.bulk_items(docs, batch_size, self.pipeline.pipeline_name)

        items = Ingest.get_items(response)
        self._verify_bulk_response(index, plan, items)
        # verify fail processor stats
        ret = self.cluster.stats.ingest()
        stats = parser(ret, ["nodes", "*", "ingest", "pipelines", self.pipeline.pipeline_name, "processors", "fail", "stats"])
        total_count_processed_by_fail_processor = sum([n["count"] for n in stats])
        total_failure = sum([n["failed"] for n in stats])
        self.assertEqual(plan.count("fail"), total_count_processed_by_fail_processor)
        self.assertEqual(plan.count("fail"), total_failure)

        # verify drop processor stats
        ret = self.cluster.stats.ingest()
        stats = parser(ret, ["nodes", "*", "ingest", "pipelines", self.pipeline.pipeline_name, "processors", "drop", "stats"])
        total_count_processed_by_drop_processor = sum([n["count"] for n in stats])
        self.assertEqual(plan.count("drop"), total_count_processed_by_drop_processor)

        # verify drop processor stats
        ret = self.cluster.stats.ingest()
        stats = parser(ret, ["nodes", "*", "ingest", "pipelines", self.pipeline.pipeline_name, "processors", "set", "stats"])
        total_count_processed_by_set_processor = sum([n["count"] for n in stats])
        self.assertEqual(plan.count("success"), total_count_processed_by_set_processor)


    def _verify_bulk_response(self, index, plan, items):
        doc = Doc(index)
        for i in range(len(plan)):
            p = plan[i]
            item_id = "batch_" + str(i+1)
            if p == "success":
                self.assertTrue("error" not in items[item_id]["index"])
                single_doc = doc.get_by_id(item_id)
                self.assertEqual(True, single_doc["_source"]["set"])
                self.assertEqual("success_" + str(i+1), single_doc["_source"]["text"])
            
            elif p == "fail":
                self.assertTrue("error" in items[item_id]["index"])
            elif p == "drop":
                self.assertTrue("error" not in items[item_id]["index"])


    def _prepare_pipeline(self, pipeline_name):

        self.pipeline = Pipeline(pipeline_name, "exception_drop_pipeline.json") 
        self.pipeline.create()

        #self.index.create(pipeline_name)

    
if __name__ == '__main__':
    unittest.main()
