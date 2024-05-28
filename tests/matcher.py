import unittest

tc = unittest.TestCase()

def process_sparse_embedding(embedding):
    """
    sort embedding by their score and only keep top tokens with highest scores
    """
    def sort_func(e):
        return e[1]
    embedding_list = [(k, embedding[k]) for k in embedding.keys()]
    embedding_list.sort(reverse=True, key=sort_func)
    return [v[0] for v in embedding_list]


def sparse_embedding_match(embedding1, embedding2):
    processed_embedding1 = process_sparse_embedding(embedding1)
    processed_embedding2 = process_sparse_embedding(embedding2)
    keep_top_n = 10
    return tc.assertEqual(processed_embedding1[:keep_top_n], processed_embedding2[:keep_top_n])

def text_embedding_match(doc1, doc2):
    source1 = doc1["_source"]
    source2 = doc2["_source"]
    tc.assertEqual(source1["text"], source2["text"])

    embedding1 = source1["text_knn"]
    embedding2 = source2["text_knn"]

    embedding1.sort(reverse=True)
    embedding2.sort(reverse=True)

    top_n = 10
    
    embedding1 = [(int)(n * 1000) for n in embedding1[:top_n]]
    embedding2 = [(int)(n * 1000) for n in embedding2[:top_n]]

    tc.assertEqual(embedding1, embedding2)
