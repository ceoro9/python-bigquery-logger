python-bigquery-logger
==========

streaming the logger info to bigquery

## Usage


Integrate a BigQueryHandler into your logging!

    >>> import logging
    >>> from bigquery_logger import BigQueryHandler
    
    >>> logger = logging.getLogger('test')
    >>> logger.setLevel(logging.DEBUG)
    
    >>> handler = BigQueryHandler('dataset ID', 'table ID')
    >>> handler.setLevel(logging.WARNING)
    >>> formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s (%(process)d): %(message)s')
    >>> handler.setFormatter(formatter)
    >>> logger.addHandler(handler)
    
    >>> logger.error("Oh noh!") # Will post the formatted message to the specified table


