import os
import uuid
import unittest
import platform
from wonderwords import RandomSentence
from basic import client
from basic.connector import Connector
from basic.model import RemoteModel, LocalModel, Model
from basic.pipeline import TextEmbeddingPipeline
from basic.ingest import Ingest
from basic.doc import Doc
from basic.index import DenseIndex
from basic.cluster import Cluster
from basic.util import parser
from tests.matcher import text_embedding_match

TEXT_EMBEDDING_PROCESSOR = "text_embedding"
BEDROCK=False
class AosIngestWithTextEmbedding(unittest.TestCase):
    maxDiff = None
    def __init__(self, *args, **kwargs):
        super(AosIngestWithTextEmbedding, self).__init__(*args, **kwargs)
        self.connector = Connector("dsf84pABqXwxfV9G8YKv")
        self.model = Model("eMf94pABqXwxfV9GJYLf")
        self.pipeline = TextEmbeddingPipeline("async-http-testing-pipeline", "eMf94pABqXwxfV9GJYLf")
        self.index = "async-http-testing-index"
        self.cluster = Cluster()

    def setUp(self):
        print("setup")
        doc = Doc(self.index)
        doc.remove_all_docs()

    def tearDown(self):
        print("start clean up...")

    def test_remote_text_embedding_batch(self):
        # ingest and verify
        self._ingest_doc_and_verify(self.index, bulk_size=3, processor_name=TEXT_EMBEDDING_PROCESSOR) 

    def test_remote_text_embedding_large_batch(self):
        # ingest and verify
        self._ingest_doc_and_verify(self.index, bulk_size=1024, processor_name=TEXT_EMBEDDING_PROCESSOR, total_doc=18) 

    @unittest.skip
    def test_remote_text_embedding_batch_with_step_size(self):
        # prepare
        step_size = 2
        # ingest and verify
        self._ingest_doc_and_verify(self.index, bulk_size=5, processor_name=TEXT_EMBEDDING_PROCESSOR, step_size=step_size)

    @unittest.skip
    def test_local_text_embedding_batch(self):
        index = str(uuid.uuid4())
        pipeline_name = str(uuid.uuid4())
        # prepare
        self._prepare_local_model_pipeline(index, pipeline_name)
        # ingest and verify
        self._ingest_doc_and_verify(index, bulk_size=5, processor_name=TEXT_EMBEDDING_PROCESSOR)

    @unittest.skip
    def test_(self):
        s = OpenAIConnector()
        s.create()
        self.assertTrue(False)

    def _get_expected_batch_size(self, doc_size, bulk_size, step_size):
        # now we don't have requests metrics for sub batches cut by step_size, return len(bulks) for now
        if BEDROCK:
            return doc_size
        return len(list(range(0, doc_size, bulk_size)))
        bulks = list(range(0, doc_size+1, bulk_size))
        if step_size is None or step_size == 0:
            return len(bulks)

        bulks.append(doc_size)
        total = 0
        for i in range(len(bulks)-1):
            total = total + len(list(range(0, bulks[i+1]-bulks[i], step_size)))
        return total

    def _ingest_doc_and_verify(self, index, bulk_size, processor_name, step_size=None, total_doc=10):
        # index through bulk
        ingest = Ingest(index)
        docs = []
        s = RandomSentence()
        for i in range(total_doc):
            docs.append({"id": "batch_"+str(i+1), "text": s.sentence()})
        ingest.bulk_items(docs, bulk_size, self.pipeline.pipeline_name)
        # verify ingest stats
        ret = self.cluster.stats.ingest()
        stats = parser(ret, ["nodes", "*", "ingest", "pipelines", self.pipeline.pipeline_name, "processors", "text_embedding", "stats"])
        total_count = sum([n["count"] for n in stats])
        total_failure = sum([n["failed"] for n in stats])
        #self.assertEqual(total_doc, total_count)
        self.assertEqual(0, total_failure)
        # verify ml stats
        ret = self.cluster.stats.ml()
        stats = parser(ret, ["nodes", "*", "models", self.model.model_id, "predict", "ml_action_request_count"])
        total_predict = sum(stats)
        # self.assertEqual(self._get_expected_batch_size(total_doc, bulk_size, step_size), total_predict)
        # verify inference data
        self._verify_inference_data(docs, index, self.pipeline.pipeline_name)

    def _verify_inference_data(self, docs, index, pipeline):
        doc = Doc(index)
        idx = [0, 9, 5, 2, 1, 3, 4, 6, 8, 7]
        for i in idx:
            doc.create_by_id(str(i+1), docs[i]["text"], pipeline)

        for i in idx:
            doc_through_single = doc.get_by_id(str(i+1))
            doc_through_bulk = doc.get_by_id("batch_"+str(i+1))
            # the float accuracy is different for the same data in a bulk request and in a single request, so we only compare keys
            text_embedding_match(doc_through_single, doc_through_bulk)
