import json
import jinja2
from basic import client, read_resource
from basic.util import wait_task, trace

class Index:
    def __init__(self, index):
        self.index = index

    @trace
    def delete(self):
        return client.http.delete(f"/{self.index}")


class SparseIndex(Index):
    def __init__(self, index):
        super().__init__(index)
        self.template =  jinja2.Template(read_resource("sparse_index.json"))

    @trace
    def create(self, pipeline):
        body = self.template.render(pipeline=pipeline)
        body = json.loads(body)
        return client.http.put(f"/{self.index}", body=body)

class DenseIndex(Index):
    def __init__(self, index, dimension):
        super().__init__(index)
        self.template = jinja2.Template(read_resource("text_embedding_index.json"))
        self.dimension = dimension

    @trace
    def create(self, pipeline):
        body = self.template.render(pipeline=pipeline, dimension=self.dimension)
        body = json.loads(body)
        return client.http.put(f"/{self.index}", body=body)

