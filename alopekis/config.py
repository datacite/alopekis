from os import getenv
from dotenv import load_dotenv

# Load environment variables from .env file or set default values
load_dotenv()

# OpenSearch settings
#OPENSEARCH_HOST = getenv('OPENSEARCH_HOST', 'vpc-elasticsearch-stage-v5cpxnwf7inua2lmmbvmtj2zra.eu-west-1.es.amazonaws.com')
OPENSEARCH_HOST = getenv('OPENSEARCH_HOST', 'vpc-elasticsearch-weeycc6fzvwkubqvkohqri73da.eu-west-1.es.amazonaws.com')
OPENSEARCH_PORT = getenv('OPENSEARCH_PORT', '80')
OPENSEARCH_INDEX = getenv('OPENSEARCH_INDEX', 'dois')

# Application settings
OUTPUT_PATH = getenv('OUTPUT_PATH', '/data/2025-may')
WORKERS = getenv('WORKERS', 32)
