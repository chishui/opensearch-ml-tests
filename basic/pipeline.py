import json
import jinja2
from basic import client, read_resource
from basic.util import wait_task, trace

class Pipeline:
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name

    @trace
    def create(self):
        res = client.http.put(f"/_ingest/pipeline/{self.pipeline_name}", body=self.get_body())
        return res

    @trace
    def delete(self):
        res = client.http.delete(f"/_ingest/pipeline/{self.pipeline_name}")
        return res


class SparseEncodingPipeline(Pipeline):
    def __init__(self, pipeline_name, model_id):
        super().__init__(pipeline_name)
        self.model_id = model_id
        self.body_template = jinja2.Template(read_resource("sparse_encoding_pipeline.json"))

    def get_body(self):
        body = self.body_template.render(model_id=self.model_id) 
        return json.loads(body)


class TextEmbeddingPipeline(Pipeline):
    def __init__(self, pipeline_name, model_id):
        super().__init__(pipeline_name)
        self.model_id = model_id
        self.body_template = jinja2.Template(read_resource("text_embedding_pipeline.json"))

    def get_body(self):
        body = self.body_template.render(model_id=self.model_id) 
        return json.loads(body)
