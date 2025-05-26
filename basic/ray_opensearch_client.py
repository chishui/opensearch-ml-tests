import ray
from basic import initialize
from retry import retry
from opensearchpy import OpenSearchException

@ray.remote
class OpenSearchClient:
    def __init__(self):
        self.client = initialize()
    
    @retry(OpenSearchException, tries=3, delay=1, backoff=2)
    def bulk(self, index, batch):
        return self.client.bulk(index=index, body="\n".join(batch), params={'refresh': 'false'})
