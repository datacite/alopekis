import calendar
from glob import iglob
from multiprocessing import Queue
import os
from .config import OUTPUT_PATH
from .opensearch import OpenSearchClient


def generate_manifest_file() -> None:
    """Generate a listing of the files within the datafile and save as MANIFEST."""
    with open(f'{OUTPUT_PATH}/MANIFEST', 'w') as manifest_file:
        for file in iglob(f'dois/*/*.gz', root_dir=OUTPUT_PATH):
            manifest_file.write(f'{file} {os.path.getsize(os.path.join(OUTPUT_PATH, file))}\n')


def get_month_count(year: int, month: int, logger=None) -> int:
    """Get the record count for single month"""
    # Prepare the client for retrieving expected counts
    agg_client = OpenSearchClient(logger=logger)
    agg_client.build_query()
    from_date = f"{year}-{month:02d}-01"
    until_date = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}"

    agg_client.query = agg_client.query.filter("range", updated={"gte": f"{from_date}T00:00:00Z",
                                                                 "lte": f"{until_date}T23:59:59Z"})
    agg_client.query = agg_client.query.extra(track_total_hits=True, size=0)
    try:
        agg_results = agg_client.query.execute()
        return agg_results.hits.total.value
    except Exception as e:
        logger.error(e)


def queue_month(year: int, month: int, work_queue: Queue, results_queue: Queue, count: int = None, logger=None) -> None:
    """Queue a month to be processed, retrieving the expected count of records if it is not provided"""
    logger.debug(f"Queueing job for {year}-{month} with expected count: {count}")
    if count:
        count = int(count)
    else:
        logger.debug(f"No count for {year}-{month} provided, querying OpenSearch")
        count = get_month_count(year, month, logger)

    work_queue.put({
        'year': int(year),
        'month': int(month),
        'count': count
    })
    results_queue.put({
        'year': int(year),
        'month': int(month),
        'count': count,
        'status': 'expected'
    })
