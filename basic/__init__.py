import os
from importlib import resources as impresources
from . import resources
from opensearchpy import OpenSearch
from dotenv import load_dotenv

dotfile = os.environ.get("DOT_FILE", None)
if dotfile != None:
    load_dotenv(dotfile)
else:
    load_dotenv()


port = 9200
host = os.environ.get('OPENSEARCH_URL', 'localhost')
auth = (os.environ.get('OPENSEARCH_USERNAME', 'admin'), os.environ.get('OPENSEARCH_PASSWORD', 'admin'))

client = OpenSearch( hosts = [{'host': host, 'port': port}], http_auth = auth, use_ssl = False, verify_certs = False)

def read_resource(resource):
    with open(impresources.files(resources) / resource, "r") as f:
        return f.read()


