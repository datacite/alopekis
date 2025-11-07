from os import getenv
from dotenv import load_dotenv

# Load environment variables from .env file or set default values
load_dotenv()

# OpenSearch settings
OPENSEARCH_HOST = getenv('OPENSEARCH_HOST', 'localhost')
OPENSEARCH_PORT = getenv('OPENSEARCH_PORT', '9203')
OPENSEARCH_INDEX = getenv('OPENSEARCH_INDEX', 'dois')

# Application settings
OUTPUT_PATH = getenv('OUTPUT_PATH', '/work/stage-test')
WORKERS = getenv('WORKERS', 8)

# METRICS
TOTAL_THRESHOLD = getenv('TOTAL_THRESHOLD', 1000)
MONTH_THRESHOLD = getenv('MONTH_THRESHOLD', 400)

# AWS Settings
DATAFILE_BUCKET = getenv('DATAFILE_BUCKET', 'datafile-stage')
LOG_BUCKET = getenv('LOG_BUCKET', 'datafile-logs')