from basic.ingest import Ingest
from basic import client
import json
import random
import string
from retry import retry
import ray
from basic.ray_opensearch_client import OpenSearchClient
from concurrent.futures import ThreadPoolExecutor
from opensearchpy.exceptions import OpenSearchException
from functools import partial
from basic.my_logger import logger
import time
import math
from collections import deque

def generate_random_string(length=10):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choices(characters, k=length))

ids = set()
class Bench:
    def __init__(self, index = None, process_size=1):
        self.index_name = index if index is not None else generate_random_string(20)
        self.futures = []
        self.ingest = Ingest(self.index_name)
        #self.client_actor = OpenSearchClient.options(num_cpus=process_size).remote()
        self.thread_pool = ThreadPoolExecutor(max_workers=process_size)
        self.process_size = process_size
        
        # Error tracking and backoff parameters
        self.error_window_size = 20  # Track last N requests
        self.error_threshold = 0.3   # If error rate exceeds this, pause
        self.recent_errors = deque(maxlen=self.error_window_size)
        self.base_backoff_time = 1.0  # Base time in seconds
        self.max_backoff_time = 60.0  # Maximum backoff time in seconds
        self.consecutive_errors = 0   # Track consecutive errors for exponential backoff

    # def ingest(self, payload_generator):
    #     doc = Doc(self.index_name)
    #     for payload in payload_generator:
    #         def create_doc():
    #             doc.create_by_id(
    #                 doc_id=None,
    #                 text=None,
    #                 body=json.dumps(payload),
    #                 refresh=False
    #             )
    #         # Submit the task to the thread pool
    #         future = self.thread_pool.submit(self._run, create_doc)
    #         self.futures.append(future)

    #     # Wait for all tasks to complete
    #     for future in self.futures:
    #         future.result()  # This will raise any exceptions that occurred
        
    #     # Clear the futures list
    #     self.futures.clear()
    #     print("ingest done")

    def _run(self, action):
        return action()
        
    def _calculate_backoff_time(self):
        """Calculate exponential backoff time based on consecutive errors"""
        backoff_time = min(self.base_backoff_time * (2 ** self.consecutive_errors), self.max_backoff_time)
        # Add some jitter to avoid thundering herd problem
        jitter = random.uniform(0, 0.1 * backoff_time)
        return backoff_time + jitter
    
    def _should_backoff(self):
        """Determine if we should back off based on recent error rate"""
        if not self.recent_errors:
            return False
            
        error_rate = sum(1 for error in self.recent_errors if error) / len(self.recent_errors)
        return error_rate >= self.error_threshold
        
    def _run_with_backoff(self, action):
        """Run an action with exponential backoff on errors"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            # Check if we should backoff based on recent error rate
            if self._should_backoff():
                backoff_time = self._calculate_backoff_time()
                logger.warning(f"High error rate detected. Backing off for {backoff_time:.2f} seconds")
                time.sleep(backoff_time)
            
            try:
                result = action()
                # Success - reset consecutive errors counter
                self.consecutive_errors = 0
                self.recent_errors.append(False)  # Record success
                return result
                
            except OpenSearchException as e:
                retry_count += 1
                self.consecutive_errors += 1
                self.recent_errors.append(True)  # Record error
                
                # Handle 429 errors with special care
                if hasattr(e, 'status_code') and e.status_code == 429:
                    backoff_time = self._calculate_backoff_time()
                    logger.warning(f"Rate limit exceeded (429). Backing off for {backoff_time:.2f} seconds")
                    time.sleep(backoff_time)
                elif retry_count < max_retries:
                    # For other errors, use a smaller backoff
                    backoff_time = self._calculate_backoff_time() / 2
                    logger.warning(f"OpenSearch error: {str(e)}. Retrying in {backoff_time:.2f} seconds")
                    time.sleep(backoff_time)
                else:
                    logger.error(f"Max retries reached. Last error: {str(e)}")
                    raise
                    
            except Exception as e:
                # For non-OpenSearch exceptions, record the error but don't retry
                self.recent_errors.append(True)
                logger.error(f"Unexpected error: {str(e)}")
                raise
    
    def batch_generator(self, data, batch_size):
        batch = []
        
        for (i, payload) in data:
            if i in ids:
                print(i)
            ids.add(i)
            batch.append(json.dumps({"index": { "_index": f"{self.index_name}", "_id": f"{i}"} }))
            batch.append(json.dumps(payload))
            if len(batch) >= batch_size * 2:
                yield batch
                batch = []
        
        # Don't forget to yield the last batch if it's not empty
        if batch:
            yield batch

    def _handle_futures(self, futures):
        error_count = 0
        total_count = len(futures)
        total_docs = 0
        updated = 0
        # Collect results first without modifying the deque
        results = []
        for future in futures:
            try:
                result = future.result()
                if result is not None:
                    if result['errors']:
                        print("********* errors ********")
                        print(result)
                        error_count += 1
                        results.append(True)  # Error
                    else:
                        results.append(False)  # Success
                        for item in result['items']:
                            total_docs += 1
                            if item["index"]["_version"] > 1:
                                updated += 1
            except Exception as e:
                print(f"An error occurred: {e}")
                error_count += 1
                results.append(True)  # Error
        
        # Now update the deque all at once
        for result in results:
            self.recent_errors.append(result)
        
        # Log error rate for monitoring
        if self.recent_errors:
            current_error_rate = sum(1 for error in self.recent_errors if error) / len(self.recent_errors)
            logger.info(f"Current error rate: {current_error_rate:.2%}")
            
        futures.clear()
        logger.info(f"total docs: {total_docs}, updated: {updated}, error: {error_count}/{total_count}")

    def bulk(self, payload_generator, bulk_size=10):
        i = 1
        total_docs = 0
        total_bulks = 0
        start_time = time.time()
        
        for batch in self.batch_generator(payload_generator, bulk_size):
            batch.append("")
            print(f'batch: {i}\r', end='', flush=True)

            def run_bulk(batch):
                return client.bulk(index=self.index_name, body="\n".join(batch), params={'refresh': 'false'})
    
            # Submit the task to the ray with backoff mechanism
            future = self.thread_pool.submit(self._run_with_backoff, partial(run_bulk, batch))
            self.futures.append(future)
            i += 1
            total_docs += bulk_size
            total_bulks += 1
            
            # Check if we need to pause due to high error rate before submitting next batch
            if self._should_backoff():
                backoff_time = self._calculate_backoff_time()
                logger.warning(f"High error rate detected. Pausing batch submissions for {backoff_time:.2f} seconds")
                time.sleep(backoff_time)
         
        self._handle_futures(self.futures)
        self.futures.clear()
        
        # Calculate and log QPS
        elapsed_time = time.time() - start_time
        qps = total_docs / elapsed_time if elapsed_time > 0 else 0
        logger.info(f"Bulk operation completed: {total_docs} documents in {elapsed_time:.2f} seconds")
        logger.info(f"Bulk QPS: {qps:.2f}")
        logger.info(f"Total bulks: {total_bulks}")
        
        print(f"bulk done, with {i-1} bulks ingested")
        
        return {
            "total_docs": total_docs,
            "elapsed_time": elapsed_time,
            "qps": qps,
            "total_bulks": total_bulks
        }

    def create_index(self, body):
        def run():
            client.index(index=self.index_name, body=body)

        self._run(run)

    def search(self, queries):
        def run(query):
            start_time = time.time()
            result = client.search(index=self.index_name, body=query)
            end_time = time.time()
            latency_ms = (end_time - start_time) * 1000  # Convert to milliseconds
            return {
                "result": result,
                "latency_ms": latency_ms
            }
        
        # check if queries is a generator
        is_generator = hasattr(queries, '__iter__') and not hasattr(queries, '__len__') and not isinstance(queries, str)
        if is_generator:
            queries = list(queries)

        if isinstance(queries, str):
            queries = [queries]
        # Store futures with their corresponding index
        results = [None] * len(queries)
        self.futures.clear()

        # Submit all queries
        for i, query in queries:
            future = self.thread_pool.submit(self._run, partial(run, query))
            self.futures.append((i, future))
        
        # Collect results in order
        for index, future in self.futures:
            results[index] = future.result()  # This will raise any exceptions that occurred
        
        # Clear the futures list
        self.futures.clear()
        print("search done")
        
        # If only one query was passed as a string, return single result
        if isinstance(queries, str):
            return results[0]
        return results 

    def __del__(self):
        # Ensure the thread pool is properly shut down
        try:
            # Use wait=False to avoid joining the current thread
            self.thread_pool.shutdown(wait=False)
        except RuntimeError:
            # Handle the case where we can't properly shut down
            pass

        if ray.is_initialized():
            ray.shutdown()
