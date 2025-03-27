from basic.doc import Doc
from basic.index import SparseIndex
from basic import client
import json
import time
import asyncio
import numpy as np

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

def test_aoss():
    doc = Doc("test-liyun")
    d = doc.get_by_id("1%3A0%3A3ZZGZZUBHtrcbbp3udZm")
    print(d)
    # Load model directly
    from transformers import AutoTokenizer, AutoModelForMaskedLM

    tokenizer = AutoTokenizer.from_pretrained("opensearch-project/opensearch-neural-sparse-encoding-doc-v2-distill")
    #model = AutoModelForMaskedLM.from_pretrained("opensearch-project/opensearch-neural-sparse-encoding-doc-v2-distill")
    ret = tokenizer.encode("hello world", return_tensors="pt")
    print(ret)

query_template = """
{
    "profile": true,
    "query": {
        "neural_sparse": {
            "passage_embedding": {
                "query_tokens": {{query}},
                "search_cluster": {{ann}},
                "document_ratio": {{ratio}},
                "sketch_type": "Sinnamon"
            }
        }
    }
}
"""
async def run_query(query):
    ret = client.search(body=query, index="test_sinnamon_ann")
    profile_results = []
    for shard in ret['profile']['shards']:
        search = shard['searches'][0]
        bool_query = search['query'][0]

        query_time = bool_query["time_in_nanos"] / 1000000
        children = bool_query['children']
        children_time = {}
        for child in children:
            children_time.setdefault(child['type'], 0)
            children_time[child['type']] += child['time_in_nanos'] / 1000000

        profile_results.append({
            "query_time": query_time,
            "children_time": children_time,
            "collect": search["collector"][0]["time_in_nanos"]/1000000
        })
    return profile_results


def simple_progress(current, total):
    percent = int((current / total) * 100)
    print(f'Progress: {percent}% [{current}/{total}]\r', end='', flush=True)


async def benchmark():
    import urllib3
    urllib3.disable_warnings()
    from sparse.dataloader import read_sparse_matrix, sparse_vector_to_json
    X = read_sparse_matrix("queries.dev.csr")

    ann = []
    normal = []
    sample_count = 100
    for i in range(sample_count):
        simple_progress(i, sample_count)
        # generate random number

        idx = np.random.randint(0, X.shape[0])

        body = sparse_vector_to_json(X[idx])
        # make two different query with different parameter and record their responses, they should be run in separate thread
        results = await asyncio.gather(
            run_query(query_template.replace("{{query}}", body).replace("{{ann}}", "true").replace("{{ratio}}", "0.1")),
            run_query(query_template.replace("{{query}}", body).replace("{{ann}}", "false").replace("{{ratio}}", "0.1"))
        )
        shard = 0
        ann.append(results[0][shard]['query_time'])
        normal.append(results[1][shard]['query_time'])

    # Replace the original print with:
    print(f"ANN mean query time: {np.mean(ann):.4f}")
    print(f"Normal mean query time: {np.mean(normal):.4f}")


if __name__ == "__main__":
    asyncio.run(benchmark())