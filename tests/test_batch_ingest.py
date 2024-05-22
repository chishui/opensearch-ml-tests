import os
import uuid
import unittest
from basic import client
from basic.connector import SageMakerConnector
from basic.model import RemoteModel
from basic.pipeline import SparseEncodingPipeline
from basic.ingest import Ingest
from basic.doc import Doc
from basic.index import Index

class BatchIngest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(BatchIngest, self).__init__(*args, **kwargs)
        self.connector = None
        self.model = None
        self.pipeline = None
        self.index = None

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

    #@unittest.skip 
    def test_batch(self):
        index = str(uuid.uuid4())
        pipeline_name = str(uuid.uuid4())

        self.index = Index(index)

        self.connector = SageMakerConnector()
        self.connector.create()

        self.model = RemoteModel(self.connector)
        self.model.register_group(str(uuid.uuid4()))
        self.model.register()
        self.model.deploy()

        self.pipeline = SparseEncodingPipeline(pipeline_name, self.model.model_id) 
        self.pipeline.create()

        ingest = Ingest(index)
        ingest.bulk(batch_size=5)

        doc = Doc(index)
        
        self.assertEqual(10, doc.count())

    @unittest.skip 
    def test_(self):
        model = RemoteModel(None)
        model.register_group()
        
            

if __name__ == '__main__':
    unittest.main()
