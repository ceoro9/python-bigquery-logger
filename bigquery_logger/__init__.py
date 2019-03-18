from logging.handlers import BufferingHandler

from google.cloud import bigquery


class BigQueryClient(object):

    @classmethod
    def new(cls, dataset_id, table_id):
        return cls(None, None, dataset_id, table_id)

    def __init__(self, service, project_id, dataset_id, table_id):
        self.client = bigquery.Client()
        self.dataset_ref = self.client.dataset(dataset_id)
        self.table_ref = self.dataset_ref.table(table_id)

    def _make_request(self, method, body):
        """Make request to API endpoint
        """
        return self.client.insert_rows_json(self.table_ref, [row['json'] for row in body['rows']])

    def insertall(self, rows):
        """
        This method insert rows into BigQuery
        """
        method = 'tabledata().insertAll().execute()'
        body = {}
        body['rows'] = [{'json': row} for row in rows]
        body["kind"] = "bigquery#tableDataInsertAllRequest"
        return self._make_request(method, body)

    def insertall_message(self, text):
        """tabledata().insertAll()

        This method insert a message into BigQuery

        Check docs for all available **params options:
        https://cloud.google.com/bigquery/docs/reference/v2/tabledata/insertAll
        """
        return self.insertall([{'logging': text}])


def get_default_service():
    from oauth2client import client
    from googleapiclient.discovery import build
    import httplib2

    credentials = client.GoogleCredentials.get_application_default()
    http = credentials.authorize(httplib2.Http())
    service = build('bigquery', 'v2', http=http)

    return service


class BigQueryHandler(BufferingHandler):
    """A logging handler that posts messages to a BigQuery channel!

    References:
    http://docs.python.org/2/library/logging.html#handler-objects
    """

    def __init__(self, dataset_id, table_id, capacity=200):
        super(BigQueryHandler, self).__init__(capacity)
        self.client = BigQueryClient.new(dataset_id, table_id)

    fields = {'created', 'filename', 'funcName', 'levelname', 'levelno', 'module', 'name', 'pathname', 'process', 'processName', 'relativeCreated', 'thread', 'threadName'}

    def mapLogRecord(self, record):
        temp = { key: getattr(record, key) for key in self.fields }
        if record.exc_info:
            temp["exc_info"] = {
                "type": unicode(record.exc_info[0]),
                "value": unicode(record.exc_info[1])
            }

        if hasattr(record, 'tags'):
            temp["tags"] = [unicode(k) for k in record.tags]

        temp["client"] = self.name
        temp["message"] = self.format(record)
        return temp

    def flush(self):
        """
        Override to implement custom flushing behaviour.

        This version just zaps the buffer to empty.
        """
        self.acquire()
        try:
            if self.buffer:
                self.client.insertall(self.mapLogRecord(k) for k in self.buffer)
            self.buffer = []
        except Exception as e:
            pass
        finally:
            self.release()
