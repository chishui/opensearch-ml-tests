import json
import jinja2
from basic import client, read_resource
from basic.util import wait_task, trace

class Doc:
    def __init__(self, index):
        self.index = index
        self.doc_template = jinja2.Template(read_resource("doc.json"))

    @trace
    def count(self):
        res = client.http.get(f"/{self.index}/_count")
        return res["count"]

    @trace
    def get_by_id(self, doc_id):
        res = client.http.get(f"/{self.index}/_doc/{doc_id}")
        return res

    @trace
    def delete_by_id(self, doc_id):
        res = client.http.delete(f"/{self.index}/_doc/{doc_id}")
        return res


    @trace
    def create_by_id(self, doc_id, text, pipeline=None):
        body = self.doc_template.render(text=text)
        url = f"/{self.index}/_doc/{doc_id}?refresh=true"
        if pipeline is not None:
            url = f"{url}&pipeline={pipeline}"
        res = client.http.post(url, body=json.loads(body))
        return res

