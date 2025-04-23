from basic.doc import Doc
from basic.index import SparseIndex
from basic import client
import json
import time
import asyncio
import numpy as np
from basic.bench import Bench

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


doc_template = """
{
    "passage_embedding": {{embedding}}
}
"""

def ingest_msmarco(size):
    import logging
    logging.basicConfig()
    def data_generator():
        from sparse.dataloader import read_sparse_matrix, sparse_vector_to_json
        X = read_sparse_matrix("base_small.csr")
        for i in range(size):
            vec = sparse_vector_to_json(X[i])
            yield json.loads(doc_template.replace("{{embedding}}", vec))
    bench = Bench("test-index")
    bench.bulk(data_generator(), 100)


if __name__ == "__main__":
    ingest_msmarco(100000)