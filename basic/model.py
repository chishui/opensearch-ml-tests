import os
import json
import jinja2
from basic import client, read_resource
from basic.util import wait_task, trace


class Model:
    def __init__(self):
        self.model_id = None
        self.model_group_id = None

    @trace
    @wait_task(timeout=60)
    def deploy(self):
        res = client.http.post(f"/_plugins/_ml/models/{self.model_id}/_deploy")
        return res

    @trace
    @wait_task(timeout=60)
    def undeploy(self):
        res = client.http.post(f"/_plugins/_ml/models/{self.model_id}/_undeploy")
        return res

    @trace
    def register_group(self, name=None):
        body = {
                "name": "test model group" if name == None else name,
                "description": "a test model group"
                }
        res = client.http.post("/_plugins/_ml/model_groups/_register", body=body)
        self.model_group_id = res["model_group_id"]
        return res

    @trace
    def delete_group(self):
        if self.model_group_id != None:
            res = client.http.delete(f"/_plugins/_ml/model_groups/{self.model_group_id}")
        self.model_group_id = None

    @trace
    def delete(self):
        if self.model_id != None:
            res = client.http.delete(f"/_plugins/_ml/models/{self.model_id}")
        self.model_id = None


class RemoteModel(Model):
    def __init__(self, connector):
        super().__init__()
        self.connector = connector
        self.body_template = jinja2.Template(read_resource("remote_model_register.json"))

    @trace
    @wait_task
    def register(self):
        body = self.body_template.render(model_name="test model name", 
                    model_group_id=self.model_group_id,
                    connector_id=self.connector.connector_id)
        body = json.loads(body)
        res = client.http.post("/_plugins/_ml/models/_register", body=body)
        self.model_id = res["model_id"]
        return res

