from basic.doc import Doc
from basic.index import SparseIndex
from basic import client
import json
import time

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

query = """
{
    "profile": true,
    "query": {
        "neural_sparse": {
            "passage_embedding": {
                "query_tokens": {"1012": 0.4382106065750122, "1037": 0.15386399626731873, "1999": 0.18573004007339478, "2004": 0.30501940846443176, "2011": 1.5390092134475708, "2017": 0.8142797946929932, "2022": 0.6884363889694214, "2043": 0.8107420802116394, "2065": 0.608598530292511, "2076": 0.2744312286376953, "2079": 1.1518434286117554, "2096": 0.663967490196228, "2108": 0.36734479665756226, "2115": 0.21437744796276093, "2128": 0.18260763585567474, "2160": 0.11151000112295151, "2173": 1.1364772319793701, "2190": 0.1745837926864624, "2302": 0.33546262979507446, "2323": 0.697616696357727, "2341": 0.14116185903549194, "2370": 1.053743600845337, "2393": 0.44559723138809204, "2442": 0.2809106707572937, "2476": 0.005995188839733601, "2477": 0.36167508363723755, "2518": 0.2015930563211441, "2535": 0.7449816465377808, "2590": 0.30661988258361816, "2610": 0.7725951075553894, "2619": 0.03775611147284508, "2681": 0.1184609904885292, "2693": 0.015904942527413368, "2711": 0.25890833139419556, "2731": 0.5179017782211304, "2894": 0.682201087474823, "2954": 0.008973398245871067, "2961": 0.3196288049221039, "2969": 0.9540621638298035, "3036": 2.200681447982788, "3102": 0.4219120740890503, "3105": 0.6305987238883972, "3153": 0.05609758570790291, "3182": 1.071822166442871, "3198": 0.09352840483188629, "3282": 0.2492436319589615, "3357": 0.9201433658599854, "3457": 1.9831994771957397, "3639": 0.13342680037021637, "3647": 0.3000773787498474, "3808": 0.6835966110229492, "3827": 0.38971030712127686, "3860": 0.7572186589241028, "4019": 0.24052435159683228, "4047": 0.6929106116294861, "4118": 0.4750722050666809, "4147": 0.21322952210903168, "4426": 1.4271466732025146, "4439": 0.14141058921813965, "4611": 0.6804642081260681, "4652": 0.1309572458267212, "4795": 0.19956472516059875, "4847": 0.26439806818962097, "4932": 1.2611671686172485, "4933": 0.013720141723752022, "5009": 0.07657672464847565, "5057": 0.4666503667831421, "5081": 0.6631625890731812, "5160": 0.0160331092774868, "5195": 0.22908444702625275, "5248": 0.21757830679416656, "5268": 0.015579280443489552, "5368": 0.12952156364917755, "5656": 0.27094483375549316, "5843": 0.19931313395500183, "5851": 1.456836462020874, "5929": 0.00305639929138124, "5933": 0.5202502012252808, "6040": 0.6404350996017456, "6107": 0.05067688226699829, "6179": 0.13627971708774567, "6204": 0.30990856885910034, "6366": 0.17690907418727875, "6458": 0.0550370030105114, "6477": 0.04072410613298416, "6912": 0.41380736231803894, "6951": 0.35990044474601746, "7216": 0.015917928889393806, "7221": 0.03674957901239395, "8066": 0.5088193416595459, "8275": 0.24335940182209015, "8325": 0.4930966794490814, "8427": 1.1525981790327933e-05, "8639": 0.10151881724596024, "8640": 0.1286291927099228, "9344": 0.06382022798061371, "9499": 0.0920027419924736, "9651": 0.0001807645458029583, "9740": 0.09543712437152863, "10412": 0.23737525939941406, "10684": 0.11207098513841629, "12913": 0.05881747603416443, "13024": 0.3297205865383148, "14046": 0.029287146404385567, "16174": 0.5561215877532959, "17019": 0.49730241298675537, "18196": 0.11880391091108322, "18405": 0.5682578682899475, "18801": 0.5326582193374634, "19519": 0.27799177169799805, "28213": 0.10487282276153564},
                "search_cluster": false,
                "document_ratio": 0.1,
                "sketch_type": "Sinnamon"
            }
        }
    }
}
"""
def run_query():
    ret = client.search(body=query, index="test_sinnamon_ann")
    search = ret['profile']['shards'][0]['searches'][0]
    bool_query = search['query'][0]

    query_time = bool_query["time_in_nanos"] / 1000000
    children = bool_query['children']
    children_time = {}
    for child in children:
        children_time.setdefault(child['type'], 0)
        children_time[child['type']] += child['time_in_nanos'] / 1000000

    print(f"query time: {query_time}")
    print(children_time)
    print(f'collect: {search["collector"][0]["time_in_nanos"]/1000000 }')

if __name__ == "__main__":
    run_query()