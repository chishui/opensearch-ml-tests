import json
import jinja2
from basic import client, read_resource
from basic.util import wait_task, trace

class Ingest:
    def __init__(self, index):
        self.index = index
        bulk_template = jinja2.Template(read_resource("bulk.json"))
        body = bulk_template.render(index=index) 
        self.bulk_body = body
        self.bulk_item_template = jinja2.Template(read_resource("bulk_item.json"))

    @trace
    def bulk(self, batch_size=None):
        url = self._get_bulk_url(batch_size)
        return client.http.post(url, body=self.bulk_body)

    @trace
    def bulk_items(self, items, batch_size=None):
        url = self._get_bulk_url(batch_size)
        body = "" 
        for item in items:
            body = body + self.bulk_item_template.render(index=self.index,
                                                  id=item["id"],
                                                  text=item["text"]) 
        return client.http.post(url, body=body)


    def _get_bulk_url(self, batch_size):
        url = "/_bulk?refresh=true"
        if batch_size != None:
            url = f"/_bulk?batch_size={batch_size}&refresh=true"
        return url
        

