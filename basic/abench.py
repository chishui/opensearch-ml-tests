import asyncio
import aiohttp
from typing import List, Tuple, Generator
import json
from contextlib import contextmanager
import time

@contextmanager
def timer(name):
    start = time.time()
    yield
    end = time.time()
    print(f"{name} took {end - start:.2f} seconds")

class AsyncBench:
    def __init__(self, index):
        self.index_name = index
        
    async def __aenter__(self):
        # Create session when entering context
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Cleanup session when exiting context
        if self.session:
            await self.session.close()

    async def _bulk_request(self, batch: List[str]) -> dict:
        batch.append("")  # Add final newline
        data = "\n".join(batch)
        
        url = f"{self.base_url}/{self.index_name}/_bulk"
        async with self.session.post(url, data=data, headers={'Content-Type': 'application/x-ndjson'}) as response:
            return await response.json()

    def batch_generator(self, data, batch_size) -> Generator[List[str], None, None]:
        batch = []
        for i, payload in data:
            batch.append(json.dumps({"index": {"_index": self.index_name, "_id": str(i)}}))
            batch.append(json.dumps(payload))
            if len(batch) >= batch_size * 2:
                yield batch
                batch = []
        if batch:
            yield batch

    async def bulk(self, payload_generator, batch_size=100, concurrent_requests=10):
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(concurrent_requests)
        
        async def process_batch(batch: List[str], batch_num: int) -> Tuple[int, dict]:
            async with semaphore:
                try:
                    result = await self._bulk_request(batch)
                    print(f'Processed batch: {batch_num}\r', end='', flush=True)
                    return batch_num, result
                except Exception as e:
                    print(f"Error in batch {batch_num}: {str(e)}")
                    return batch_num, {"error": str(e)}

        # Create all batches
        batches = list(self.batch_generator(payload_generator, batch_size))
        
        # Create tasks for all batches
        tasks = [
            process_batch(batch, i) 
            for i, batch in enumerate(batches, 1)
        ]

        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks)
        
        # Process results
        successful_docs = 0
        updated_docs = 0
        
        for _, result in sorted(results, key=lambda x: x[0]):
            if not result.get('errors'):
                for item in result['items']:
                    successful_docs += 1
                    if item["index"]["_version"] > 1:
                        updated_docs += 1

        print(f"Bulk operation completed: {successful_docs} docs ingested, {updated_docs} updated")
        return successful_docs, updated_docs

    async def search(self, queries, concurrent_requests=10):
        semaphore = asyncio.Semaphore(concurrent_requests)
        
        async def execute_search(query):
            async with semaphore:
                url = f"{self.base_url}/{self.index_name}/_search"
                async with self.session.post(url, json=query) as response:
                    return await response.json()

        if isinstance(queries, str):
            queries = [queries]

        tasks = [execute_search(query) for query in queries]
        return await asyncio.gather(*tasks)

# Usage example:
async def main():
    async with AsyncBench("test-index") as bench:
        # Generate some test data
        test_data = [
            (i, {"field1": f"value{i}", "field2": i}) 
            for i in range(1000)
        ]
        
        with timer("Bulk insert"):
            docs_inserted, docs_updated = await bench.bulk(
                test_data,
                batch_size=100,
                concurrent_requests=10
            )
            
        # Example search
        queries = [
            {"query": {"match": {"field1": "value1"}}},
            {"query": {"match": {"field1": "value2"}}}
        ]
        
        with timer("Search queries"):
            search_results = await bench.search(queries)

# Run the async code
if __name__ == "__main__":
    asyncio.run(main())
