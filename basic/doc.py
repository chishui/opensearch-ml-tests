import json
import jinja2
from basic import client, read_resource
from basic.util import wait_task, trace

class Doc:
    def __init__(self, index):
        self.index = index

    @trace
    def count(self):
        res = client.http.get(f"/{self.index}/_count")
        return res["count"]
