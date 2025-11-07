import calendar
import logging
import time

from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearch_dsl import Search
from .config import OPENSEARCH_HOST, OPENSEARCH_PORT, OPENSEARCH_INDEX
from .exceptions import TooManyFailures, TooManyTimeouts


class OpenSearchClient:
    def __init__(self, logger=None):
        """
        Initialize an OpenSearchClient object.

        This method creates an `OpenSearch` client object with settings from the config file.
        """
        self.opensearch_client = OpenSearch(
            hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
            http_compress=True,
            http_auth=None,
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            connection_class=RequestsHttpConnection
        )
        # TODO: Configure a more generous default timeout
        self.query = None
        self.logger = logger

        # Disable the logs about connections to the OpenSearch cluster and urllib3 debug messages
        logging.getLogger("opensearch").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.INFO)
        #logger.propagate = False

    def return_all_results(self):
        """Execute query and return all results as an iterator, using search_after"""
        timeout_count = 0
        failure_count = 0
        if not self.query: self.build_query()
        query_finished = False

        while not query_finished:
            search_after = self.query.to_dict().get("search_after")
            #self.logger.debug(f"Running query with `search_after`: {search_after}")
            #self.logger.debug(f"Query: {self.query.to_dict()}")
            try:
                response = self.query.execute()
                if not response.timed_out:
                    # TODO: Check this actually triggers, Timeout exceptions might be unintentionally caught by the broad `except Exception`
                    #       block below and therefore counting as a Failure rather than a timeout
                    if len(response.hits) > 0:
                        #if len(response.hits) < 1000:
                            #self.logger.debug(f"LESS THAN 1K - Query returned {len(response.hits)} results")
                        yield from response.hits
                        self.query = self.query.extra(search_after=response.hits[-1].meta.sort)
                    else:
                        query_finished = True
                else:
                    timeout_count += 1
                    self.logger.info(f"Query timed out (count: {timeout_count}): {self.query.to_dict}")
                    if timeout_count > 10:
                        self.logger.error("Too many timeouts, giving up")
                        raise TooManyTimeouts
                    else:
                        self.logger.warning("Timeout, sleeping for 10 seconds and retrying")
                        time.sleep(10)
            except Exception as e:
                failure_count += 1
                if failure_count > 10:
                    self.logger.error("Too many failures, giving up")
                    raise TooManyFailures
                else:
                    self.logger.warning("Failure, sleeping for 10 seconds and retrying")
                    self.logger.error(f"Error message: {e}")
                    time.sleep(10)

    def build_query(self):
        """Build a basic query to match all findable or registered DataCite DOIs"""
        s = Search(using=self.opensearch_client, index=OPENSEARCH_INDEX)
        s = s.filter("terms", agency=["DataCite", "datacite"])
        s = s.filter("terms", aasm_state=["findable", "registered"])
        #s = s.filter("range", updated={"lte": "2020-01-01T00:00:00Z"})  #TEMP for testing
        s = s.query()  # This adds a simple match_all
        s = s.sort("updated", "uid")
        s = s.extra(track_total_hits=False, size=1000)
        self.query = s


    def filter_fields(self):
        """Add filters to the query object to limit results to specific fields"""
        field_list = [
            "uid",
            "prefix",
            "suffix",
            "identifiers",
            "creators",
            "titles",
            "publisher_obj",
            "container",
            "publication_year",
            "subjects",
            "contributors",
            "dates",
            "language",
            "types",
            "related_identifiers",
            "related_items",
            "sizes",
            "formats",
            "version_info",
            "rights_list",
            "descriptions",
            "geo_locations",
            "funding_references",
            "url",
            "content_url",
            "metadata_version",
            "schema_version",
            "source",
            "is_active",
            "aasm_state",
            "reason",
            "view_count",
            "views_over_time",
            "download_count",
            "downloads_over_time",
            "reference_count",
            "citation_count",
            "citations_over_time",
            "part_count",
            "part_of_count",
            "version_count",
            "version_of_count",
            "created",
            "registered",
            "published",
            "updated",
            "client_id",
            "provider_id",
            "media_ids",
            "reference_ids",
            "citation_ids",
            "part_ids",
            "part_of_ids",
            "version_ids",
            "version_of_ids",
        ]
        if not self.query: self.build_query()
        self.query = self.query.source(includes=field_list)

    def add_month_filter(self, year, month):
        """Add a filter to the query object to limit results to records updated in a certain year and month"""
        if not self.query: self.build_query()
        self.query = self.query.filter("range", updated={"gte": f"{year}-{month:02d}-01T00:00:00Z",
                                                         "lte": f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}"
                                                         f"T23:59:59Z"})