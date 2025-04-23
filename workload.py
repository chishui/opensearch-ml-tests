import json
import random

def random_query():
    with open("output.json", "r") as f:
        queries = json.load(f)
        print(queries[0])
        query = random.choice(queries)
        return {
            "body": {
                "query": {
                    "match": {
                        "passage_text": query
                    }
                }
            }
        }

random_query()