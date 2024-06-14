
test_local_batch:
	DOT_FILE="/Users/xiliyun/projects/opensearch-ml-tests/basic/.localhost.env" DEBUG=1 pytest tests/test_batch_ingest.py

test_local_sparse:
	DOT_FILE="/Users/xiliyun/projects/opensearch-ml-tests/basic/.localhost.env" DEBUG=1 pytest tests/test_batch_ingest_sparseencoding.py

test_local_dense:
	DOT_FILE="/Users/xiliyun/projects/opensearch-ml-tests/basic/.localhost.env" DEBUG=1 pytest tests/test_batch_ingest_textembedding.py

test_remote:
	DEBUG=1 pytest

test:
	pytest

