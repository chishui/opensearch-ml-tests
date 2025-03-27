import os
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
from importlib import resources as impresources
from . import resources
from dotenv import load_dotenv

dotfile = os.environ.get("DOT_FILE", None)
print(f"env file: {dotfile}")
if dotfile != None:
    load_dotenv(dotfile)
else:
    load_dotenv()


port = int(os.environ.get('OPENSEARCH_PORT', '9200'))
host = os.environ.get('OPENSEARCH_URL', 'localhost')
auth = (os.environ.get('OPENSEARCH_USERNAME', 'admin'), os.environ.get('OPENSEARCH_PASSWORD', 'admin'))
USE_SSL = os.environ.get('SSL', '0') == '1'
USE_AWS = os.environ.get('AWS', '0') == '1'
if USE_AWS:
    aws_auth = AWS4Auth(
        os.environ.get('AWS_ACCESS_KEY', ''), 
        os.environ.get('AWS_SECRET_KEY', ''), 
        os.environ.get('AWS_REGION', 'us-east-1'), 
        os.environ.get('AWS_SERVICE', 'opensearch')
    )
    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_auth = aws_auth,
        use_ssl = USE_SSL,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
elif os.environ.get('OPENSEARCH_USERNAME', '') == '' and os.environ.get('OPENSEARCH_PASSWORD', '') == '':
    client = OpenSearch( hosts = [{'host': host, 'port': port}], http_auth=None, use_ssl = USE_SSL, verify_certs = False)
else:
    client = OpenSearch( hosts = [{'host': host, 'port': port}], http_auth=auth, use_ssl = USE_SSL, verify_certs = False)

def read_resource(resource):
    with open(impresources.files(resources) / resource, "r") as f:
        return f.read()


