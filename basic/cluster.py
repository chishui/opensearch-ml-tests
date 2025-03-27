import os
import json
import jinja2
from basic import client, read_resource
from basic.util import trace

class Cluster:
    def __init__(self):
        self.settings = Cluster.Settings()
        self.stats = Cluster.Stats()


    class Settings:
        def __init__(self):
            self.ml_commons_setting_template = jinja2.Template(read_resource("ml_commons_settings.json"))

        @trace
        def ml_commons(self, only_run_on_ml_node):
            body = self.ml_commons_setting_template.render(only_run_on_ml_node="true" if only_run_on_ml_node == True else "false")
            body = json.loads(body)
            return client.http.put("/_cluster/settings", body=body)

    class Stats:
        def __init__(self):
            pass

        @trace
        def ingest(self):
            return client.http.get("/_nodes/stats/ingest")

        @trace
        def ml(self):
            return client.http.get("/_plugins/_ml/stats")

        @trace
        def jvm(self):
            return client.http.get("/_nodes/stats/jvm")

