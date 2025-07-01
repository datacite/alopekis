from os import getenv
from dotenv import load_dotenv

# Load environment variables from .env file or set default values
load_dotenv()

# OpenSearch settings
OPENSEARCH_HOST = getenv('OPENSEARCH_HOST', 'localhost')
OPENSEARCH_PORT = getenv('OPENSEARCH_PORT', '9203')
OPENSEARCH_INDEX = getenv('OPENSEARCH_INDEX', 'dois')

# Application settings
OUTPUT_PATH = getenv('OUTPUT_PATH', '/work/pidgraph/staging1k')
WORKERS = getenv('WORKERS', 8)