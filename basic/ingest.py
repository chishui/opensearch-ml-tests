import json
import jinja2
import sys
from basic import client, read_resource
from basic.util import wait_task, trace, logger

class Ingest:
    def __init__(self, index):
        self.index = index
        bulk_template = jinja2.Template(read_resource("bulk.json"))
        body = bulk_template.render(index=index)
        self.bulk_body = body
        self.bulk_item_template = jinja2.Template(read_resource("bulk_item.json"))

    @trace
    def bulk(self, batch_size=None, pipeline=None):
        url = self._get_bulk_url(batch_size, pipeline)
        return client.http.post(url, body=self.bulk_body)

    def bulk_items(self, items, batch_size=None, pipeline=None):
        url = self._get_bulk_url(batch_size, pipeline)
        body = ""
        for item in items:
            body = body + self.bulk_item_template.render(index=self.index,
                                                  id=item["id"],
                                                  text=item["text"])
        
        logger.info(f"bulk payload size: {sys.getsizeof(body)}")
        return client.http.post(url, body=body)


    def _get_bulk_url(self, batch_size, pipeline):
        url = "/_bulk?refresh=true"
        if batch_size is not None:
            url = f"{url}&batch_size={batch_size}"
        if pipeline is not None:
            url = f"{url}&pipeline={pipeline}"
        return url

    @staticmethod
    def get_items(response):
        items = {}
        for item in response["items"]:
            items[item["index"]["_id"]] = item
        return items

