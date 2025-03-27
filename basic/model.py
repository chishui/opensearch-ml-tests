import os
import json
from functools import partial
import jinja2
from basic import client, read_resource
from basic.util import wait_task, trace


class Model:
    def __init__(self, model_id=None):
        self.model_id = model_id
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

    def set_model_id(self, res):
        if "model_id" in res:
            self.model_id = res["model_id"]

    @trace
    @wait_task(callback=set_model_id)
    def register(self):
        body = self._get_body()
        res = client.http.post("/_plugins/_ml/models/_register", body=body)
        return res


class RemoteModel(Model):
    def __init__(self, connector):
        super().__init__()
        self.connector = connector
        self.body_template = jinja2.Template(read_resource("remote_model_register.json"))

    def _get_body(self):
        body = self.body_template.render(model_name="test model name", 
                    model_group_id=self.model_group_id,
                    connector_id=self.connector.connector_id)
        return json.loads(body)


class LocalModel(Model):
    def __init__(self, model_config="local_sparse_model.json"):
        super().__init__()
        self.body = json.loads(read_resource(model_config))

    def _get_body(self):
        self.body["model_group_id"] = self.model_group_id
        return self.body

