import logging
from logging.handlers import QueueHandler
from multiprocessing import Process, JoinableQueue
from queue import Queue
from datetime import datetime
from dateutil import rrule
import threading

from alopekis.config import WORKERS
from alopekis.opensearch import OpenSearchClient
from alopekis.worker import month_worker


def logging_thread(log_queue: Queue) -> None:
    """Thread that handles logging

    Args:
        log_queue (Queue): Queue containing log messages.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    fhandler = logging.FileHandler(f'{datetime.utcnow()}.log')
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


def results_thread(results_queue: Queue, log_queue: Queue) -> None:
    """Thread that handles results

    Args:
        results_queue (Queue): Queue containing results.
    """
    queue_handler = QueueHandler(log_queue)
    logger = logging.getLogger(f"results")
    logger.addHandler(queue_handler)
    logger.setLevel(logging.INFO)

    results = {}
    while True:
        result = results_queue.get(block=True)
        logger.info(f"Got result: {result}")
        if result is None:
            logger.info("Got None, writing CSV and stopping...")
            # Write results to file
            with open('results.csv', 'w') as f:
                for key, value in results.items():
                    f.write(f"{key},{value['expected']},{value['final']}\n")
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


if __name__ == "__main__":
    log_queue = Queue()
    log_thread = threading.Thread(target=logging_thread, args=(log_queue,))
    log_thread.start()

    queue_handler = QueueHandler(log_queue)
    logger = logging.getLogger(f"main")
    logger.addHandler(queue_handler)
    logger.setLevel(logging.INFO)

    results_queue = JoinableQueue()
    results_thread = threading.Thread(target=results_thread, args=(results_queue, log_queue,))
    results_thread.start()

    work_queue = JoinableQueue()

    workers = []
    for i in range(WORKERS):
        wp = Process(target=month_worker, args=(i, work_queue, results_queue, log_queue))
        workers.append(wp)
        wp.start()

    # Get the expected counts
    agg_client = OpenSearchClient(logger=logger)
    agg_client.build_query()
    agg_client.query = agg_client.query.extra(track_total_hits=True, size=0)
    agg_client.query.aggs.bucket('updated', 'date_histogram', field='updated', calendar_interval='month', format='yyyy-MM')

    try:
        agg_results = agg_client.query.execute()
    except Exception as e:
        logger.error(e)
        exit(1)

    for bucket in agg_results.aggregations.updated.buckets:
        year, month = bucket.key_as_string.split('-')
        work_queue.put({
            'year': int(year),
            'month': int(month),
            'count': bucket.doc_count
        })
        results_queue.put({
            'year': int(year),
            'month': int(month),
            'count': bucket.doc_count,
            'status': 'expected'
        })

    logger.info(f"Expected total count: {agg_results.hits.total.value}")

    # # Populate queue with jobs
    # # 2017-12 - 2024-09
    # start_date = datetime.strptime('2017-12', '%Y-%m')
    # end_date = datetime.strptime('2024-09', '%Y-%m')
    # for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
    #     work_queue.put({
    #         'year': dt.year,
    #         'month': dt.month,
    #         'count': 0
    #     })

    # Populate queue with stop signal
    for i in range(WORKERS):
        work_queue.put(None)

    # Wait for workers to finish
    for wp in workers:
        wp.join()

    # Shut down results thread
    results_queue.put(None)
    results_thread.join()

    # Shut down logging thread
    log_queue.put(None)
    log_thread.join()