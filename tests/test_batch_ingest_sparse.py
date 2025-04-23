import os
import json
import uuid
import time
import unittest
import itertools
from wonderwords import RandomSentence
from basic import client
from basic.connector import SageMakerConnector
from basic.model import RemoteModel, LocalModel
from basic.pipeline import SparseEncodingPipeline
from basic.ingest import Ingest
from basic.doc import Doc
from basic.index import SparseIndex
from basic.cluster import Cluster
from basic.util import parser
from tests.matcher import sparse_embedding_match

SPARSE_PROCESSOR = "sparse_encoding"

class BatchIngest(unittest.TestCase):
    maxDiff = None
    def __init__(self, *args, **kwargs):
        super(BatchIngest, self).__init__(*args, **kwargs)
        self.connector = None
        self.model = None
        self.pipeline = None
        self.index = None
        self.cluster = Cluster()

    def setUp(self):
        print("setup")

    def tearDown(self):
        print("start clean up...")
        if self.model != None:
            self.model.undeploy()
            self.model.delete()
            self.model.delete_group()
        if self.pipeline != None:
            self.pipeline.delete()
        if self.connector != None:
            self.connector.delete()
        if self.index != None:
            self.index.delete()

        self.connector = None
        self.model = None
        self.pipeline = None
        self.index = None

    def test_remote_sparse_batch(self):
        index = str(uuid.uuid4())
        pipeline_name = str(uuid.uuid4())
        # prepare
        self._prepare_connector_model_pipeline(index, pipeline_name)
        # ingest and verify
        self._ingest_doc_and_verify(index, batch_size=3, processor_name=SPARSE_PROCESSOR) 

    def test_large_remote_sparse_batch(self):
        index = str(uuid.uuid4())
        pipeline_name = str(uuid.uuid4())
        # prepare
        self._prepare_connector_model_pipeline(index, pipeline_name)
        # ingest and verify
        self._ingest_doc_and_verify(index, batch_size=102, processor_name=SPARSE_PROCESSOR, total_doc=1024) 

    def test_remote_sparse_batch_with_step_size(self):
        index = str(uuid.uuid4())
        pipeline_name = str(uuid.uuid4())
        # prepare
        step_size = 2
        self._prepare_connector_model_pipeline(index, pipeline_name, step_size=step_size)
        # ingest and verify
        self._ingest_doc_and_verify(index, batch_size=5, processor_name=SPARSE_PROCESSOR, step_size=step_size)

    def test_local_sparse_batch(self):
        index = str(uuid.uuid4())
        pipeline_name = str(uuid.uuid4())
        # prepare
        self._prepare_local_model_pipeline(index, pipeline_name)
        # ingest and verify
        self._ingest_doc_and_verify(index, batch_size=5, processor_name=SPARSE_PROCESSOR)

    @unittest.skip
    def test_(self):
        s = SageMakerConnector(2)
        s.create()
        self.assertTrue(False)

    def _get_expected_batch_size(self, doc_size, batch_size, step_size):
        # now we don't have requests metrics for sub batches cut by step_size, return len(bulks) for now
        return len(list(range(0, doc_size, batch_size)))
        bulks = list(range(0, doc_size+1, batch_size))
        if step_size is None or step_size == 0:
            return len(bulks)

        bulks.append(doc_size)
        total = 0
        for i in range(len(bulks)-1):
            total = total + len(list(range(0, bulks[i+1]-bulks[i], step_size)))
        return total

    def _ingest_doc_and_verify(self, index, batch_size, processor_name, step_size=None, total_doc=10):
        # index through bulk
        ingest = Ingest(index)
        docs = []
        s = RandomSentence()
        for i in range(total_doc):
            docs.append({"id": "batch_"+str(i+1), "text": s.sentence()})
        ingest.bulk_items(docs, batch_size, self.pipeline.pipeline_name)
        # verify ingest stats
        ret = self.cluster.stats.ingest()
        stats = parser(ret, ["nodes", "*", "ingest", "pipelines", self.pipeline.pipeline_name, "processors", "sparse_encoding", "stats"])
        total_count = sum([n["count"] for n in stats])
        total_failure = sum([n["failed"] for n in stats])
        self.assertEqual(total_doc, total_count)
        self.assertEqual(0, total_failure)
        # verify ml stats
        ret = self.cluster.stats.ml()
        stats = parser(ret, ["nodes", "*", "models", self.model.model_id, "predict", "ml_action_request_count"])
        total_predict = sum(stats)
        self.assertEqual(self._get_expected_batch_size(total_doc, batch_size, step_size), total_predict)
        # verify inference data
        self._verify_inference_data(docs, index, self.pipeline.pipeline_name)

    def _verify_inference_data(self, docs, index, pipeline):
        doc = Doc(index)
        idx = [0, 9, 5]
        for i in idx:
            doc.create_by_id(str(i+1), docs[i]["text"], pipeline)

        for i in idx:
            doc_through_single = doc.get_by_id(str(i+1))
            doc_through_bulk = doc.get_by_id("batch_"+str(i+1))
            # the float accuracy is different for the same data in a bulk request and in a single request, so we only compare keys
            sparse_embedding_match(doc_through_single["_source"]["passage_embedding"], doc_through_bulk["_source"]["passage_embedding"])


    def _prepare_connector_model_pipeline(self, index, pipeline_name, step_size=None):
        self.index = SparseIndex(index)

        self.connector = SageMakerConnector(step_size=step_size)
        self.connector.create()

        self.model = RemoteModel(self.connector)
        self.model.register_group(str(uuid.uuid4()))
        self.model.register()
        self.model.deploy()

        self.pipeline = SparseEncodingPipeline(pipeline_name, self.model.model_id) 
        self.pipeline.create()

        self.index.create(pipeline_name)

    def _prepare_local_model_pipeline(self, index, pipeline_name):
        self.cluster.settings.ml_commons(only_run_on_ml_node=False)

        self.index = SparseIndex(index)

        self.model = LocalModel()
        self.model.register_group(str(uuid.uuid4()))
        self.model.register()
        self.model.deploy()

        self.pipeline = SparseEncodingPipeline(pipeline_name, self.model.model_id) 
        self.pipeline.create()

        self.index.create(pipeline_name)


if __name__ == '__main__':
    unittest.main()
