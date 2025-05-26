from basic.doc import Doc
from basic.index import SparseIndex
from basic import client
import json
import time
import asyncio
import numpy as np
from basic.bench import Bench
import click
from basic.my_logger import logger
import statistics

@click.group()
def cli():
    """Main command group"""
    pass

def run():
    doc = Doc("test-liyun")
    with open("/Users/xiliyun/Downloads/corpus.jsonl", "r") as f:
        total = 10000
        i = 0
        for line in f:
            line = json.loads(line)
            text = line["text"]
            embedding = line["sparse_embedding"]
            body = { "passage_text": text, "passage_embedding": embedding }
            success = False
            retry = 10
            while success == False and retry > 0:
                try:
                    doc.create_by_id(None, text=None, body=json.dumps(body), refresh=True)
                    success = True
                except Exception as e:
                    print(e)
                    time.sleep(10)
                    retry = retry - 1
            
            i = i + 1
            if i >= total:
                break
            
    # for i in range(1000):
    #     doc_id = f"aa_{i}"
    #     if i < 200:
    #         body = { "passage_embedding": {"cow": 3.0} }
    #         doc.create_by_id(doc_id, text=None, body=json.dumps(body), refresh=True)
    #     elif i < 500:
    #         body = { "passage_embedding": {"apple": 3.0} }
    #         doc.create_by_id(doc_id, text=None, body=json.dumps(body), refresh=True)
    #     else:
    #         body = { "passage_embedding": {"orange": 2.0} }
    #         doc.create_by_id(doc_id, text=None, body=json.dumps(body), refresh=True)

def prepare_query():
    output = []
    with open("/Users/xiliyun/Downloads/corpus.jsonl", "r") as f:
        total = 10000
        i = 0
        for line in f:
            line = json.loads(line)
            text = line["text"]
            output.append(" ".join(text.split(" ")[:10]))
    with open("output.json", "w") as fout:
        fout.write(json.dumps(output))


query_template = """
{
    "query": {
        "sparse_ann": {
            "passage_embedding": {
                "query_tokens": {{query}}
            }
        }
    }
}
"""
async def run_query(query):
    ret = client.search(body=query, index="test-index")
    profile_results = {
        "took": ret['took'],
        "profile": []
    }
    if 'profile' not in ret:
        return profile_results
    for shard in ret['profile']['shards']:
        search = shard['searches'][0]
        bool_query = search['query'][0]

        query_time = bool_query["time_in_nanos"] / 1000000
        children = bool_query['children']
        children_time = {}
        for child in children:
            children_time.setdefault(child['type'], 0)
            children_time[child['type']] += child['time_in_nanos'] / 1000000

        profile_results['profile'].append({
            "query_time": query_time,
            "children_time": children_time,
            "collect": search["collector"][0]["time_in_nanos"]/1000000
        })
    return profile_results


def simple_progress(current, total):
    percent = int((current / total) * 100)
    print(f'Progress: {percent}% [{current}/{total}]\r', end='', flush=True)
    if percent % 10 == 0:  # Log every 10% progress
        logger.info(f'Progress: {percent}% [{current}/{total}]')


doc_template = """
{
    "passage_embedding": {{embedding}}
}
"""

def render_template(template, params):
    for k, v in params.items():
        template = template.replace("{{" + k + "}}", v)
    return json.loads(template)

def sparse_vector_generator(file, size, transform = None):
    from sparse.dataloader import read_sparse_matrix, sparse_vector_to_json
    X = read_sparse_matrix(file)
    if size == 0:
        size = X.shape[0]
    for i in range(0, size):
        vec = sparse_vector_to_json(X[i % X.shape[0]])
        if transform is None:
            yield (i, vec)
        else:
            yield (i, transform(vec))

@cli.command()
@click.option("--index-name", help="index name")
@click.option("--data-file", help="data file name")
@click.option("--size", help="size", type=int, default=0)
@click.option("--bulk-size", help="bulk size", type=int, default=1000)
@click.option("--clients", help="number of clients", type=int, default=1)
def ingest_msmarco(index_name, data_file, size, bulk_size, clients):
    logger.info(f"Starting ingestion with index={index_name}, data_file={data_file}, size={size}, bulk_size={bulk_size}, clients={clients}")
    bench = Bench(index_name, process_size=clients)
    bench.bulk(sparse_vector_generator(data_file, size, 
                                       lambda v : render_template(doc_template, {"embedding": v})), bulk_size)
    logger.info("Ingestion completed")


index_template = """
{
  "settings": {
      "index": {
          "sparse": true,
          "number_of_shards": 1
      }
  },
  "mappings": {
    "properties": {
      "sparse_embedding":{
          "type": "rank_features"
      },
      "passage_embedding":{
          "type": "sparse_tokens",
          "method": {
              "name": "seismic",
              "lambda": 20,
              "alpha": 0.4,
              "beta": 5
          }
      }
    }
  }
}
"""

sparse_query_template = """
{  
    "_source": {
        "excludes": [
            "sparse_embedding"
        ]
    },
    "query": {
        "neural_sparse": {
            "sparse_embedding": {
                "query_tokens": {{embedding}}
            }
        }
    },
    "size": 10
}
"""

sparse_ann_query_template = """
{
    "_source": false,
    "query": {
        "sparse_ann": {
            "passage_embedding": {
                "query_tokens": {{embedding}},
                "cut": {{cut}},
                "heap_factor": 1.2
            }
        }
    },
    "size": 10
}
"""

def create_index():
    index = SparseIndex("test-index")
    index.create(index_template)

def query_compare(size):
    bench = Bench("test-index")
    sparse_queries = sparse_vector_generator("query.csr", size, lambda v : sparse_query_template.replace("{{vector}}", v))
    ann_queries = sparse_vector_generator("query.csr", size, lambda v : sparse_ann_query_template.replace("{{vector}}", v))
  
    sparse_results = bench.search(sparse_queries)
    ann_results = bench.search(ann_queries)
    
    # Calculate and display latency statistics for sparse queries
    sparse_latency_stats = calculate_latency_stats(sparse_results)
    logger.info("Sparse Query Latency Report:")
    logger.info(f"  Total Queries: {sparse_latency_stats['count']}")
    logger.info(f"  Mean Latency: {sparse_latency_stats['mean']:.2f} ms")
    logger.info(f"  P50 Latency: {sparse_latency_stats['p50']:.2f} ms")
    logger.info(f"  P90 Latency: {sparse_latency_stats['p90']:.2f} ms")
    logger.info(f"  P99 Latency: {sparse_latency_stats['p99']:.2f} ms")
    
    # Calculate and display latency statistics for ANN queries
    ann_latency_stats = calculate_latency_stats(ann_results)
    logger.info("ANN Query Latency Report:")
    logger.info(f"  Total Queries: {ann_latency_stats['count']}")
    logger.info(f"  Mean Latency: {ann_latency_stats['mean']:.2f} ms")
    logger.info(f"  P50 Latency: {ann_latency_stats['p50']:.2f} ms")
    logger.info(f"  P90 Latency: {ann_latency_stats['p90']:.2f} ms")
    logger.info(f"  P99 Latency: {ann_latency_stats['p99']:.2f} ms")


def evaluate(data, truth_file, k=10):
    #logger.info(f"Evaluating results against truth file: {truth_file}")
    correct = np.loadtxt(truth_file, delimiter=",").astype(int)
    #logger.info(f"Loaded truth data with shape: {correct.shape}")
    
    recalls = np.zeros(len(data))
    for i in range(len(data)):
        # Convert both to Python int for consistent comparison
        #logger.debug(f"{i}th truth entry: {correct[i]}")
        #logger.debug(f"{i}th result entry: {data[i]}")
        data_set = set(int(x) for x in data[i])
        correct_set = set(int(x) for x in correct[i])
        recalls[i] = len(data_set & correct_set)
        #logger.debug(f"{i}th recall: {recalls[i]}\n")
    
    ret = np.mean(recalls) / float(k)
    logger.info(f"Evaluation complete. Mean recall@{k}: {ret:.4f}")
    return ret

def calculate_latency_stats(results):
    """
    Calculate latency statistics from search results
    
    Args:
        results: List of search results with latency information
        
    Returns:
        Dictionary containing mean, p50, p90, and p99 latency statistics
    """
    latencies = [result['latency_ms'] for result in results]
    
    if not latencies:
        return {
            'mean': 0,
            'p50': 0,
            'p90': 0,
            'p99': 0,
            'count': 0
        }
    
    return {
        'mean': statistics.mean(latencies),
        'p50': statistics.median(latencies),
        'p90': statistics.quantiles(latencies, n=10)[8],  # 9th decile (90%)
        'p99': statistics.quantiles(latencies, n=100)[98],  # 99th percentile
        'count': len(latencies)
    }

@cli.command()
@click.option("--index-name", help="index name")
@click.option("--data-file", help="data file name")
@click.option("--size", help="size", type=int, default=0)
@click.option("--clients", help="number of clients", type=int, default=1)
@click.option("--start", help="start from query index", type=int, default=0)
@click.option("--warmup", help="number of warmup queries to run before benchmarking", type=int, default=0)
def query(index_name, data_file, size, clients, start, warmup):
    logger.info(f"Starting query with index={index_name}, data_file={data_file}, size={size}, clients={clients}, warmup={warmup}")
    bench = Bench(index_name, process_size=clients)
    
    # Run warmup queries if specified
    if warmup > 0:
        logger.info(f"Running {warmup} warmup queries...")
        # Generate a subset of queries for warmup
        warmup_queries = list(sparse_vector_generator(data_file, min(warmup, size), 
                                       lambda v : render_template(sparse_ann_query_template, {"embedding": v, "cut": "3"})))
        
        # Run warmup queries
        warmup_results = bench.search(warmup_queries)
        logger.info(f"Warmup completed with {len(warmup_results)} queries")
    
    # Run actual benchmark queries
    results = bench.search(sparse_vector_generator(data_file, size, 
                                       lambda v : render_template(sparse_ann_query_template, {"embedding": v, "cut": "3"})))
    logger.info(f"Search completed, processing results")
    
    # Calculate and display latency statistics
    latency_stats = calculate_latency_stats(results)
    logger.info("Latency Report:")
    logger.info(f"  Total Queries: {latency_stats['count']}")
    logger.info(f"  Mean Latency: {latency_stats['mean']:.2f} ms")
    logger.info(f"  P50 Latency: {latency_stats['p50']:.2f} ms")
    logger.info(f"  P90 Latency: {latency_stats['p90']:.2f} ms")
    logger.info(f"  P99 Latency: {latency_stats['p99']:.2f} ms")
    
    # Extract search results for evaluation
    data = []
    for i, result_with_latency in enumerate(results):
        result = result_with_latency['result']  # Extract the actual search result
        one_data = []
        for hit in result['hits']['hits']:
            one_data.append(int(hit['_id']))
        data.append(one_data if len(one_data) > 0 else [-1])

    evaluate(data, 'brutal_array.txt', k=10)


if __name__ == "__main__":
    cli()
