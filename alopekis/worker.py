import gzip
import logging
import os
from logging.handlers import QueueHandler
from queue import Queue
from csv import DictWriter

from ujson import dumps

from .config import OUTPUT_PATH
from .opensearch import OpenSearchClient
from .serializer import json_serialize, csv_serialize
from .exceptions import FatalWorkerError


def month_worker(worker_id: int, work_queue: Queue, results_queue: Queue, log_queue: Queue) -> None:
    """Retrieve job from the queue and process it

    Args:
        worker_id (int): ID of the worker for logging.
        work_queue (Queue): Queue containing jobs.
        results_queue (Queue): Queue to use for reporting results.
        log_queue (Queue): Queue to use for logging.
    """
    queue_handler = QueueHandler(log_queue)
    logger = logging.getLogger(f"worker-{worker_id}")
    # logger.propagate = False
    logger.addHandler(queue_handler)
    logger.debug(f"Worker {worker_id} started")

    while True:
        job = work_queue.get()
        if job is None:
            logger.info(f"Worker {worker_id} received None, stopping...")
            break

        # Parse the job information
        year = job['year']
        month = job['month']
        expected_count = job['count']
        logger.info(f"Worker {worker_id} started processing job for {year}-{month} with expected count {expected_count}")

        # Process the job

        # Make sure output directory exists
        output_dir = f"{OUTPUT_PATH}/dois/updated_{year}-{month:02d}"
        try:
              if not os.path.exists(output_dir):
                os.makedirs(output_dir)
        except Exception as e:
            logger.error(f"Worker {worker_id} failed to create output directory {output_dir}: {e}")
            raise FatalWorkerError

        client = OpenSearchClient(logger=logger)
        client.build_query()
        client.filter_fields()
        client.add_month_filter(year, month)

        results_count = 0
        current_file_index = 0

        # Open the output files
        json_file_path = f"{OUTPUT_PATH}/dois/updated_{year}-{month:02d}/part_{current_file_index:04d}.jsonl.gz"
        csv_file_path = f"{OUTPUT_PATH}/dois/updated_{year}-{month:02d}/{year}-{month:02d}.csv.gz"
        try:
            # Write direct to gzip
            json_output_file = gzip.open(json_file_path, "wt")
        except Exception as e:
            logger.error(f"Worker {worker_id} failed to open file {json_file_path} for writing: {e}")
            raise FatalWorkerError

        try:
            csv_output_file = gzip.open(csv_file_path, "wt")
            csv_writer = DictWriter(csv_output_file, fieldnames=["doi", "state", "client_id", "updated"])
            csv_writer.writeheader()
        except Exception as e:
            logger.error(f"Worker {worker_id} failed to open file {csv_file_path} for writing: {e}")
            raise FatalWorkerError

        try:
            results = client.return_all_results()

            for result in results:
                results_count += 1

                # Write everything to the CSV
                csv_writer.writerow(csv_serialize(result))

                # Only write to JSONL if the record is findable
                if result.aasm_state == "findable":
                    serialized_record = json_serialize(result)
                    json_output_file.write(f"{dumps(serialized_record, escape_forward_slashes=False, ensure_ascii=False)}\n")
                    json_output_file.flush()

                if expected_count > 1000000 and results_count % 100000 == 0:
                    logger.info(f"Worker {worker_id} processed {results_count}/{expected_count} records for {year}-{month}")

                if results_count % 10000 == 0:
                    logger.debug(f"Worker {worker_id} processed {results_count}/{expected_count} records for {year}-{month}")
                    current_file_index += 1
                    try:
                        # Close the JSONL file and open the next file
                        json_output_file.close()
                        json_output_file = gzip.open(json_file_path, "wt")
                    except Exception as e:
                        logger.error(f"Worker {worker_id} failed to open file {json_file_path} for writing: {e}")
                        raise FatalWorkerError

            # Close the last files and report results
            csv_output_file.close()
            json_output_file.close()
            results_queue.put({"year": year, "month": month, "count": results_count, "status": "final"}, block=True)
            logger.info(f"Worker {worker_id} finished processing job for {year}-{month} with final count {results_count}")
            work_queue.task_done()

        except Exception as e:
            logger.error(f"Worker {worker_id} failed to process job for {year}-{month}: {e}")
            raise FatalWorkerError
