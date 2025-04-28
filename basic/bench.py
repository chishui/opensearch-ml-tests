from basic.doc import Doc
from basic.ingest import Ingest
from basic import client
import json
import random
import string
from retry import retry
from opensearchpy import OpenSearchException
from concurrent.futures import ThreadPoolExecutor


def generate_random_string(length=10):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choices(characters, k=length))


def simple_progress(current, total):
    percent = int((current / total) * 100)
    print(f'Progress: {percent}% [{current}/{total}]\r', end='', flush=True)


class Bench:
    def __init__(self, index = None, ingest_thread_pool_size=10, search_thread_pool_size=10):
        self.index_name = index if index is not None else generate_random_string(20)
        self.thread_pool = ThreadPoolExecutor(max_workers=ingest_thread_pool_size)
        self.futures = []
        self.search_thread_pool = ThreadPoolExecutor(max_workers=search_thread_pool_size)
        self.search_futures = []

    @retry(OpenSearchException, tries=10, delay=1, backoff=2, max_delay=30)
    def _run(self, action):
        return action()

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
        
        for (i, payload) in data:
            print(i)
            batch.append(json.dumps({"index": { "_index": f"{self.index_name}", "_id": f"{i}"} }))
            batch.append(json.dumps(payload))
            if len(batch) >= batch_size * 2:
                yield batch
                batch = []
        
        # Don't forget to yield the last batch if it's not empty
        if batch:
            yield batch

    def bulk(self, payload_generator, bulk_size=10):
        ingest = Ingest(self.index_name)
        i = 1
        for batch in self.batch_generator(payload_generator, bulk_size):
            batch.append("")
            print(f'batch: {i}\r', end='', flush=True)
            def run_bulk():
                return ingest.bulk(body="\n".join(batch), refresh=False)
            # Submit the task to the thread pool
            future = self.thread_pool.submit(self._run, run_bulk)
            self.futures.append(future)
            i += 1
        print("total size of future: ", len(self.futures))
        # Wait for all tasks to complete
        i = 1
        all_results = []
        for future in self.futures:
            result = future.result()  # This will raise any exceptions that occurred
            if result is not None:
                if result['errors']:
                    print("********* errors ********")
                    print(result)
                else:
                    all_results += [(item["index"]["_id"], item["index"]["_version"]) for item in result['items']]
 
            print(f'batch result: {i}\r', end='', flush=True)
            i += 1
        
        # Clear the futures list
        updated = [item for item in all_results if item[1] > 1]
        self.futures.clear()
        print(f"bulk done, with {len(all_results)} docs ingested, {len(updated)} updated")   

    def create_index(self, body):
        def run():
            client.index(index=self.index_name, body=body)

        self._run(run)

    def search(self, queries):
        def run(query):
            return client.search(index=self.index_name, body=query)
        if isinstance(queries, str):
            queries = [queries]
        # Store futures with their corresponding index
        results = [None] * len(queries)
        self.search_futures.clear()

        # Submit all queries
        for i, query in enumerate(queries):
            future = self.search_thread_pool.submit(self._run, run(query))
            self.search_futures.append((i, future))
        
        # Collect results in order
        for index, future in self.search_futures:
            results[index] = future.result()  # This will raise any exceptions that occurred
        
        # Clear the futures list
        self.search_futures.clear()
        print("search done")
        
        # If only one query was passed as a string, return single result
        if isinstance(queries, str):
            return results[0]
        return results 

    def __del__(self):
        # Ensure the thread pool is properly shut down
        self.thread_pool.shutdown(wait=True)
        self.search_thread_pool.shutdown(wait=True)


