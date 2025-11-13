import logging
import argparse
import calendar
from glob import iglob
from logging.handlers import QueueHandler
from multiprocessing import Process, JoinableQueue
from queue import Queue
from datetime import datetime, date #, UTC    # UTC is new in Python 3.13 so this was erroring in prod
import threading

from alopekis.config import WORKERS, DATAFILE_BUCKET, OUTPUT_PATH, LOG_BUCKET, TOTAL_THRESHOLD, MONTH_THRESHOLD
from alopekis.opensearch import OpenSearchClient
from alopekis.s3 import empty_bucket, put_files
from alopekis.utils import generate_manifest_file, queue_month
from alopekis.worker import month_worker
from time import sleep


def logging_thread(log_queue: Queue, local=False) -> None:
    """Thread that handles logging

    Args:
        log_queue (Queue): Queue containing log messages.
        local (bool): Store logs locally only, don't upload to S3.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    logfile = f'{datetime.utcnow()}.log'    # TODO: Replace this with datetime.now(datetime.UTC) once we require py3.13 as minimum
    fhandler = logging.FileHandler(logfile)
    fhandler.setLevel(logging.DEBUG)
    shandler = logging.StreamHandler()
    shandler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fhandler.setFormatter(formatter)
    shandler.setFormatter(formatter)

    logger.addHandler(fhandler)
    logger.addHandler(shandler)

    while True:
        log = log_queue.get()
        if log is None:
            break
        logger.handle(log)

    if not local:
        # Upload log file and results CSV to S3
        put_files(files=[logfile, "results.csv"], bucket=LOG_BUCKET, extra_args={'ContentType': 'text/plain'})


def results_thread(results_queue: Queue, work_queue: Queue, worker_count: int, log_queue: Queue) -> None:
    """Thread that handles results

    Args:
        results_queue (Queue): Queue containing results.
        work_queue (Queue): Queue for submitting regeneration jobs.
        worker_count (int): Number of workers (required for sending shutdown signal).
        log_queue (Queue): Queue to use for logging.
    """
    queue_handler = QueueHandler(log_queue)
    logger = logging.getLogger(f"results")
    logger.propagate = False
    logger.addHandler(queue_handler)
    logger.setLevel(logging.INFO)
    results = {}
    while True:
        result = results_queue.get(block=True)
        logger.debug(f"Got result: {result}")
        if result is None:
            logger.debug("Got None, writing CSV and stopping...")
            # Write results to file
            # TODO: Name this in line with the logfile so we can keep it for analysis
            with open('results.csv', 'w') as f:
                for key, value in results.items():
                    f.write(f"{key},{value['expected']},{value['final']},{value['diff']},{value['pct']:0.5f}\n")
            break

        year = result['year']
        month = result['month']
        count = result['count']
        status = result['status']

        key = f"{year}-{month}"
        if key not in results:
            results[key] = {}
        else:
            if status in results[key]:
                logger.warning(f"Duplicate status {status} for {key}. Old value: {results[key][status]}, new value: {count}")

        results[key][status] = count

        if status == "final":
            results[key]['diff'] = results[key]['expected'] - results[key]['final']
            results[key]['pct'] = ((results[key]['diff'] / results[key]['expected']) * 100) if results[key]['expected'] > 0 else 0

            # Check if all results are done
            if all(["final" in results[key] for key in results]):
                logger.info("All results have 'final', analysing!")
                current_key = date.today().strftime("%Y-%m")
                discrepancy = sum(results[key]['diff'] for key in results if key != current_key)
                logger.info(f"Total result discrepancy: {discrepancy}")
                if discrepancy > TOTAL_THRESHOLD:
                    logger.info(f"Discrepancy above threshold, selecting months with a difference of more than {MONTH_THRESHOLD} for regeneration")
                    months_with_discrepancy = [key for key in results if results[key]['diff'] > 0]
                    logger.info("Discrepancies:")
                    for m in months_with_discrepancy:
                        logger.info(f"{m} - Expected: {results[m]['expected']} - Final: {results[m]['final']} - Diff: {results[m]['diff']} - Threshold: {results[m]['diff'] > MONTH_THRESHOLD}")
                    months_to_rerun = [key for key in results if results[key]['diff'] > MONTH_THRESHOLD]

                    if len(months_to_rerun) > 0:
                        logger.info(f"Regenerating {len(months_to_rerun)} months: {months_to_rerun}")

                        for key in months_to_rerun:
                            # del results[key]['final']
                            # del results[key]['diff']
                            # del results[key]['pct']
                            # year, month = key.split('-')
                            # work_queue.put({
                            #     'year': int(year),
                            #     'month': int(month),
                            #     'count': results[key]['expected']
                            # })
                            del results[key]
                            year, month = key.split('-')
                            queue_month(year=int(year),
                                        month=int(month),
                                        work_queue=work_queue,
                                        results_queue=results_queue,
                                        count=None,  # Force a requery of expected count from OpenSearch
                                        logger=logger)
                    else:
                        logger.info("No months to rerun, shutting down workers and commencing packaging")
                        sleep(1)  # Necessary to prevent workers closing before the main thread has joined them
                        for _ in range(worker_count):
                            work_queue.put(None)

                else:
                    logger.info("Discrepancy in acceptable range, shutting down workers and commencing packaging")
                    sleep(1)  # Necessary to prevent workers closing before the main thread has joined them
                    for _ in range(worker_count):
                        work_queue.put(None)


if __name__ == "__main__":
    # Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', "--verbose", action="store_true", help="Increase output verbosity")
    parser.add_argument('-w', "--workers", type=int, default=WORKERS, help="Override worker count")
    parser.add_argument('-l', '--local', action="store_true", help="Generate locally only, don't upload to S3")
    parser.add_argument("--from-date", type=str, default=None, help="Set start date of generation query (YYYY-MM-DD)")
    parser.add_argument("--until-date", type=str, default=None, help="Set end date of generation query (YYYY-MM-DD)")
    parser.add_argument("--single", type=str, default=None, help="Shortcut to regenerate an individual month (YYYY-MM)")
    args = parser.parse_args()

    # Start the thread that handles logging
    log_queue = Queue()
    log_thread = threading.Thread(target=logging_thread, args=(log_queue, args.local,))
    log_thread.start()

    # Set up the main logger
    queue_handler = QueueHandler(log_queue)
    logger = logging.getLogger(f"main")
    logger.addHandler(queue_handler)
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    logger.propagate = False
    logger.info("Data File Generation started...")
    logger.info(f"Called with arguments: {args}")

    worker_count = int(args.workers) if args.workers else int(WORKERS)

    # Set up the queues used for handing out jobs and processing results
    work_queue = JoinableQueue()
    results_queue = JoinableQueue()
    results_thread = threading.Thread(target=results_thread, args=(results_queue, work_queue, worker_count, log_queue,))
    results_thread.start()

    workers = []
    for i in range(worker_count):
        wp = Process(target=month_worker, args=(i, work_queue, results_queue, log_queue))
        workers.append(wp)
        wp.start()

    # Prepare the client for retrieving expected counts
    agg_client = OpenSearchClient(logger=logger)
    agg_client.build_query()

    # Add any date limitations from command line arguments
    try:
        if args.from_date:
            from_date = date.fromisoformat(args.from_date)
        else:
            from_date = None
    except ValueError:
        exit("Invalid from_date format")

    try:
        if args.until_date:
            until_date = date.fromisoformat(args.until_date)
        else:
            until_date = None
    except ValueError:
        exit("Invalid to_date format")

    if args.single:
        if args.from_date or args.to_date:  # TODO: See if we ca do this with ArgParse
            exit("--single is mutually exclusive with --from-date and --until-date")
        year, month = args.single.split("-")
        year = int(year)
        month = int(month)

        from_date = f"{year}-{month:02d}-01"
        until_date = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}"

    if from_date and until_date:
        agg_client.query = agg_client.query.filter("range", updated={"gte": f"{from_date}T00:00:00Z",
                                                                     "lte": f"{until_date}T23:59:59Z"})
    elif from_date and not until_date:
        agg_client.query = agg_client.query.filter("range", updated={"gte": f"{from_date}T00:00:00Z"})

    elif until_date and not from_date:
        agg_client.query = agg_client.query.filter("range", updated={"lte": f"{until_date}T23:59:59Z"})

    agg_client.query = agg_client.query.extra(track_total_hits=True, size=0)
    agg_client.query.aggs.bucket('updated', 'date_histogram', field='updated', calendar_interval='month', format='yyyy-MM')

    try:
        agg_results = agg_client.query.execute()
        for bucket in agg_results.aggregations.updated.buckets:
            year, month = bucket.key_as_string.split('-')
            queue_month(year=int(year),
                        month=int(month),
                        work_queue=work_queue,
                        results_queue=results_queue,
                        count=bucket.doc_count,
                        logger=logger)
            # work_queue.put({
            #     'year': int(year),
            #     'month': int(month),
            #     'count': bucket.doc_count
            # })
            # results_queue.put({
            #     'year': int(year),
            #     'month': int(month),
            #     'count': bucket.doc_count,
            #     'status': 'expected'
            # })

        logger.info(f"Expected total count: {agg_results.hits.total.value}")
    except Exception as e:
        logger.error(e)

        # TODO: Shutdown workers and prevent packaging etc?

    finally:

        # Wait for workers to finish
        for wp in workers:
            wp.join()
        logger.info("Data File generation finished!")

        # Shut down results thread
        results_queue.put(None)
        results_thread.join()

        # Generate the manifest file
        logger.info("Generating MANIFEST file")
        generate_manifest_file()

        if not args.local:
            # Clear S3 Bucket of old data file
            logger.info(f"Clearing S3 bucket: {DATAFILE_BUCKET}")
            empty_bucket(DATAFILE_BUCKET)

            # Upload new data file to S3
            logger.info("Uploading new data file")
            put_files(files=iglob(f'dois/*/*.gz', root_dir=OUTPUT_PATH), bucket=DATAFILE_BUCKET, extra_args={'ContentType': 'application/gzip'}, root_dir=OUTPUT_PATH)
            put_files(files=['MANIFEST'], bucket=DATAFILE_BUCKET, extra_args={'ContentType': 'text/plain'}, root_dir=OUTPUT_PATH)
            logger.info("Data file upload complete")

        logger.info(f"Process complete, shutting down log thread{' and uploading logs to S3' if not args.local else ''}")
        # Shut down logging thread
        log_queue.put(None)
        log_thread.join()
