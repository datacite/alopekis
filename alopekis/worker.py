import gzip
import logging
import os
from csv import DictWriter
from logging.handlers import QueueHandler
from multiprocessing import JoinableQueue, Queue

from ujson import dumps

from .config import OUTPUT_PATH
from .exceptions import FatalWorkerError
from .opensearch import OpenSearchClient
from .serializer import csv_serialize, json_serialize


def process_month_job(
    worker_id: int,
    job: dict,
    work_queue: JoinableQueue,
    results_queue: JoinableQueue,
    logger: logging.Logger,
) -> None:
    """Process a single month job. This contains the previous inner logic extracted from the
    original month_worker for handling a single job. It does not call work_queue.task_done(); the
    caller (wrapper) is responsible for that so it can requeue failed jobs if needed.

    Raises:
        FatalWorkerError: When a non-recoverable error occurs while processing the job.
    """
    # Parse the job information
    year = job["year"]
    month = job["month"]
    expected_count = job["count"]
    logger.info(
        f"Worker {worker_id} started processing job for {year}-{month} with expected count {expected_count}"
    )

    # Make sure output directory exists
    output_dir = f"{OUTPUT_PATH}/dois/updated_{year}-{month:02d}"
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    except Exception as e:
        logger.error(
            f"Worker {worker_id} failed to create output directory {output_dir}: {e}"
        )
        raise FatalWorkerError

    client = OpenSearchClient(logger=logger)
    client.build_query()
    client.filter_fields()
    client.add_month_filter(year, month)

    results_count = 0
    findable_count = 0
    registered_count = 0

    current_file_index = 0

    # Open the output files
    json_file_path = f"{OUTPUT_PATH}/dois/updated_{year}-{month:02d}/part_{current_file_index:04d}.jsonl.gz"
    csv_file_path = (
        f"{OUTPUT_PATH}/dois/updated_{year}-{month:02d}/{year}-{month:02d}.csv.gz"
    )
    try:
        # Write direct to gzip
        json_output_file = gzip.open(json_file_path, "wt")
    except Exception as e:
        logger.error(
            f"Worker {worker_id} failed to open file {json_file_path} for writing: {e}"
        )
        raise FatalWorkerError

    try:
        csv_output_file = gzip.open(csv_file_path, "wt")
        csv_writer = DictWriter(
            csv_output_file, fieldnames=["doi", "state", "client_id", "updated"]
        )
        csv_writer.writeheader()
    except Exception as e:
        logger.error(
            f"Worker {worker_id} failed to open file {csv_file_path} for writing: {e}"
        )
        # Ensure opened JSON file is closed before raising
        try:
            json_output_file.close()
        except Exception:
            pass
        raise FatalWorkerError

    try:
        results = client.return_all_results()

        for result in results:
            results_count += 1

            # Write everything to the CSV
            try:
                csv_writer.writerow(csv_serialize(result))
                if result.aasm_state == "registered":
                    registered_count += 1
            except Exception as e:
                logger.error(f"Failed to serialize record {result.uid} to CSV: {e}")

            # Only write to JSONL if the record is findable
            if result.aasm_state == "findable":
                try:
                    serialized_record = json_serialize(result)
                    json_output_file.write(
                        f"{dumps(serialized_record, escape_forward_slashes=False, ensure_ascii=False)}\n"
                    )
                    json_output_file.flush()
                    findable_count += 1
                except Exception as e:
                    logger.error(
                        f"Failed to serialize record {result.uid} to JSON: {e}"
                    )

            if results_count % 10000 == 0:
                # For long-running months, increase log messages for easier tracking during generation
                if (
                    expected_count
                    and expected_count >= 1000000
                    and results_count % 200000 == 0
                ) or (
                    expected_count
                    and 100000 <= expected_count < 1000000
                    and results_count % 50000 == 0
                ):
                    logger.info(
                        f"Worker {worker_id} processed {results_count}/{expected_count} records for {year}-{month}"
                    )
                else:
                    logger.debug(
                        f"Worker {worker_id} processed {results_count}/{expected_count} records for {year}-{month}"
                    )
                current_file_index += 1
                json_file_path = f"{OUTPUT_PATH}/dois/updated_{year}-{month:02d}/part_{current_file_index:04d}.jsonl.gz"
                try:
                    # Close the JSONL file and open the next file
                    json_output_file.close()
                    json_output_file = gzip.open(json_file_path, "wt")
                except Exception as e:
                    logger.error(
                        f"Worker {worker_id} failed to open file {json_file_path} for writing: {e}"
                    )
                    raise FatalWorkerError

        # Close the last files and report results
        try:
            csv_output_file.close()
        except Exception:
            pass
        try:
            json_output_file.close()
        except Exception:
            pass

        results_queue.put(
            {
                "year": year,
                "month": month,
                "count": results_count,
                "status": "final",
                "registered": registered_count,
                "findable": findable_count,
            },
            block=True,
        )
        logger.info(
            f"Worker {worker_id} finished processing job for {year}-{month} with final count {results_count}"
        )

    except Exception as e:
        logger.error(
            f"Worker {worker_id} failed to process job for {year}-{month}: {e}"
        )
        # Make sure file handles are closed on error
        try:
            csv_output_file.close()
        except Exception:
            pass
        try:
            json_output_file.close()
        except Exception:
            pass
        raise FatalWorkerError


def month_worker(
    worker_id: int,
    work_queue: JoinableQueue,
    results_queue: JoinableQueue,
    log_queue: Queue,
    max_retries: int = 3,
) -> None:
    """Wrapper worker function that handles logger setup, queue processing and retries.

    This function is the public entrypoint used by the main process. It pulls jobs from the
    work_queue and delegates processing to `process_month_job`. If a FatalWorkerError is raised
    during processing, the wrapper will report the failure to the results queue.
    """
    queue_handler = QueueHandler(log_queue)
    logger = logging.getLogger(f"worker-{worker_id}")
    logger.addHandler(queue_handler)
    logger.setLevel(logging.DEBUG)
    logger.debug(f"Worker {worker_id} started")

    while True:
        job = work_queue.get()
        if job is None:
            logger.info(f"Worker {worker_id} received None, stopping...")
            # Pass the sentinel on (don't call task_done for sentinel)
            break

        try:
            process_month_job(worker_id, job, work_queue, results_queue, logger)
            # Mark the job as done only on success
            try:
                work_queue.task_done()
            except Exception:
                # Some queue implementations may not have task_done available in the test harness
                logger.debug("work_queue.task_done() not available or failed")

        except FatalWorkerError:
            logger.error(
                f"Worker {worker_id} encountered a fatal error processing {job.get('year')}-{job.get('month')}, sending failure status to results queue"
            )
            try:
                results_queue.put(
                    {
                        "year": job.get("year"),
                        "month": job.get("month"),
                        "count": 0,
                        "status": "failed",
                    },
                    block=True,
                )
            except Exception:
                logger.debug("Failed to report failed job to results queue")
            # Mark the task as done so the queue accounting is correct
            try:
                work_queue.task_done()
            except Exception:
                logger.debug("work_queue.task_done() not available or failed")
