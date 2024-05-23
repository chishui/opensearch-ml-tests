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

    @trace
    def create(self, pipeline, template_file="sparse_index.json"):
        template = jinja2.Template(read_resource(template_file))
        body = template.render(pipeline=pipeline)
        body = json.loads(body)
        return client.http.put(f"/{self.index}", body=body)
