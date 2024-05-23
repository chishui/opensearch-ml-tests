test_local:
	DOT_FILE="~/basic/.localhost.env" DEBUG=1 pytest -s -v

test_remote:
	DEBUG=1 pytest -s -v

test:
	pytest

