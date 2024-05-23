import os
import json
import jinja2
from basic import client, read_resource
from basic.util import trace

class Connector:
    def __init__(self):
        self.connector_id = None
        self.body = None

    @trace
    def delete(self):
        if self.connector_id != None:
            res = client.http.delete(f"/_plugins/_ml/connectors/{self.connector_id}")
            self.connector_id = None
            return res

    @trace
    def create(self):
        res = client.http.post("/_plugins/_ml/connectors/_create", body=self.body)
        if "connector_id" in res:
            self.connector_id = res["connector_id"]
        return res


class SageMakerConnector(Connector):
    def __init__(self, step_size=None):
        super().__init__()
        other_parameters = ""
        if step_size != None:
            other_parameters = f'\n"input_docs_processed_step_size": {step_size},\n'
        body = jinja2.Template(read_resource("sagemaker_connector.json"))
        body = body.render(aws_region=os.getenv("AWS_REGION"), 
                    aws_access_key=os.getenv("AWS_ACCESS_KEY"),
                    aws_secret_key=os.getenv("AWS_SECRET_KEY"),
                    aws_endpoint=os.getenv("SAGEMAKER_ENDPOINT"),
                           other_parameters=other_parameters)
        self.body = json.loads(body)
        self.connector_id = None



