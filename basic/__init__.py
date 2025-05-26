import os
from opensearchpy import OpenSearch, RequestsHttpConnection, ConnectionPool
from requests_aws4auth import AWS4Auth
import boto3
from importlib import resources as impresources
from . import resources
from dotenv import load_dotenv
import warnings
import urllib3

def initialize():
    # Suppress the specific warning
    warnings.filterwarnings("ignore", category=UserWarning, 
                        message="Connecting to .* using SSL with verify_certs=False is insecure.")

    # Also suppress related urllib3 warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    timeout = int(os.environ.get('OPENSEARCH_TIMEOUT', '60'))

    common_args = {
        'hosts' : [{'host': host, 'port': port}],
        'connection_class': RequestsHttpConnection,
        'pool_options' : {
            'maxsize': 25,  # Increase the connection pool size
            'retry_on_timeout': True,
            'timeout': timeout
        },
        'timeout': timeout,  # Overall request timeout
        'retry_on_timeout': True,
        'max_retries': 5,
        'retry_on_status': (502, 503, 504, 429)
    }

    if USE_AWS:
        aws_auth = AWS4Auth(
            os.environ.get('AWS_ACCESS_KEY', ''), 
            os.environ.get('AWS_SECRET_KEY', ''), 
            os.environ.get('AWS_REGION', 'us-east-1'), 
            os.environ.get('AWS_SERVICE', 'opensearch')
        )
        client = OpenSearch(
            **common_args,
            http_auth = aws_auth,
            use_ssl = USE_SSL,
            verify_certs = True,
        )
    elif os.environ.get('OPENSEARCH_USERNAME', '') == '' and os.environ.get('OPENSEARCH_PASSWORD', '') == '':
        client = OpenSearch( **common_args, http_auth=None, use_ssl = USE_SSL, verify_certs = False)
    else:
        client = OpenSearch( **common_args, http_auth=auth, use_ssl = USE_SSL, verify_certs = False)

    print(client.info())
    return client

client = initialize()

def read_resource(resource):
    with open(impresources.files(resources) / resource, "r") as f:
        return f.read()


