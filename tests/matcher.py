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

