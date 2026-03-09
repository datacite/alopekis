import calendar
from datetime import UTC, date, datetime, timedelta
from multiprocessing import Queue

from ujson import dump, dumps

from .config import OUTPUT_PATH
from .opensearch import OpenSearchClient
from .s3 import put_status_file


def generate_manifest_files(files) -> None:
    """Generate plai tand txe JSON listings of the files within the datafile and save as MANIFEST/MANIFEST.json."""
    to_write = []
    for file in files:
        filename, size, checksum, success = file
        if success:
            to_write.append({"filename": filename, "size": size, "sha256": checksum})

    with open(f"{OUTPUT_PATH}/MANIFEST", "w") as manifest_file:
        for entry in to_write:
            manifest_file.write(
                f'{entry["filename"]} {entry["size"]} {entry["sha256"]}\n'
            )

    with open(f"{OUTPUT_PATH}/MANIFEST.json", "w") as json_manifest_file:
        dump(to_write, json_manifest_file, indent=2, escape_forward_slashes=False)


def get_month_count(year: int, month: int, logger=None) -> int:
    """Get the record count for single month"""
    # Prepare the client for retrieving expected counts
    agg_client = OpenSearchClient(logger=logger)
    agg_client.build_query()
    from_date = f"{year}-{month:02d}-01"
    until_date = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}"

    agg_client.query = agg_client.query.filter(
        "range",
        updated={"gte": f"{from_date}T00:00:00Z", "lte": f"{until_date}T23:59:59Z"},
    )
    agg_client.query = agg_client.query.extra(track_total_hits=True, size=0)
    try:
        agg_results = agg_client.query.execute()
        return agg_results.hits.total.value
    except Exception as e:
        logger.error(e)


def queue_month(
    year: int,
    month: int,
    work_queue: Queue,
    results_queue: Queue,
    count: int = None,
    logger=None,
) -> None:
    """Queue a month to be processed, retrieving the expected count of records if it is not provided"""
    logger.info(f"Queueing job for {year}-{month} with expected count: {count}")
    if count:
        count = int(count)
    else:
        logger.info(f"No count for {year}-{month} provided, querying OpenSearch")
        count = get_month_count(year, month, logger)

    work_queue.put({"year": int(year), "month": int(month), "count": count})
    results_queue.put(
        {"year": int(year), "month": int(month), "count": count, "status": "expected"}
    )


def update_status(status: str) -> None:
    """~Write status to STATUS.json in the S3 bucket"""
    status_json = {
        "month": (date.today() - timedelta(weeks=4)).strftime("%Y-%m"),
        "datetime": datetime.now(UTC).isoformat(),
        "status": status,
    }
    put_status_file(dumps(status_json, indent=2).encode())
