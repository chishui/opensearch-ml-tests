from basic.doc import Doc
from basic.ingest import Ingest
from basic import client
import json
import time
import asyncio
import numpy as np
from retry import retry
from opensearchpy import OpenSearchException
from concurrent.futures import ThreadPoolExecutor


class Bench:
    def __init__(self, index, ingest_thread_pool_size=10):
        self.index_name = index
        self.thread_pool = ThreadPoolExecutor(max_workers=ingest_thread_pool_size)
        self.futures = []

    @retry(OpenSearchException, tries=10, delay=10)
    def _run(self, action):
        action()

    def ingest(self, payload_generator):
        doc = Doc(self.index_name)
        for payload in payload_generator:
            def create_doc():
                doc.create_by_id(
                    doc_id=None,
                    text=None,
                    body=json.dumps(payload),
                    refresh=False
                )
            # Submit the task to the thread pool
            future = self.thread_pool.submit(self._run, create_doc)
            self.futures.append(future)

        # Wait for all tasks to complete
        for future in self.futures:
            future.result()  # This will raise any exceptions that occurred
        
        # Clear the futures list
        self.futures.clear()
        print("ingest done")

    def batch_generator(self, data, batch_size):
        batch = []
        
        for payload in data:
            batch.append(json.dumps({"index": { "_index": f"{self.index_name}"} }))
            batch.append(json.dumps(payload))
            
            if len(batch) >= batch_size * 2:
                yield batch
                batch = []
        
        # Don't forget to yield the last batch if it's not empty
        if batch:
            yield batch

    def bulk(self, payload_generator, bulk_size=10):
        ingest = Ingest(self.index_name)
        for batch in self.batch_generator(payload_generator, bulk_size):
            batch.append("")
            def run_bulk():
                ingest.bulk(body="\n".join(batch), refresh=False)
            # Submit the task to the thread pool
            future = self.thread_pool.submit(self._run, run_bulk)
            self.futures.append(future)

        # Wait for all tasks to complete
        for future in self.futures:
            future.result()  # This will raise any exceptions that occurred
        
        # Clear the futures list
        self.futures.clear()
        print("bulk done")   

    def __del__(self):
        # Ensure the thread pool is properly shut down
        self.thread_pool.shutdown(wait=True)


