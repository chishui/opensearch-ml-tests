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
