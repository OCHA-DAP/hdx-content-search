"""Crawl HDX to find 3W datasets mentioning a keyword.
CKAN API documentation: http://docs.ckan.org/en/latest/api/
Python CKAN library: https://github.com/ckan/ckanapi

Started by David Megginson, 2018-03-01
"""

import ckanapi, time, sys, csv, requests, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DELAY = 1
"""Time delay in seconds between datasets, to give HDX a break."""

CHUNK_SIZE=100
"""Number of datasets to read at once"""

CKAN_URL = 'https://data.humdata.org'
"""Base URL for the CKAN instance."""

# Open a connection to HDX
ckan = ckanapi.RemoteCKAN(CKAN_URL)

# Open a CSV output stream
output = csv.writer(sys.stdout)

# Search keywords inside resource binary content
keywords = (
    b'Education',
    b'education',
)

def scan_for_keyword(package):
    """Scan all binary resources in a dataset for a binary keyword"""
    for resource in package['resources']:
        logger.debug('Processing resource %s', resource['name'])
        with requests.get(resource['url']) as result:
            for keyword in keywords:
                if result.content.find(keyword):
                    return True
    return False

output.writerow([
    'Dataset',
    'Locations',
    'HDX link'
])

# Iterate through all the datasets ("packages") and resources on HDX
start = 0
result_count = 999999 # just a big, big number; will reset on first search result
while start < result_count:
    result = ckan.action.package_search(fq='tags:3w', start=start, rows=CHUNK_SIZE)
    result_count = result['count']
    logger.info("Read %d package(s)...", len(result['results']))
    for package in result['results']:
        logger.debug('Processing package %s', package['name'])
        if scan_for_keyword(package):
            output.writerow([
                package['title'],
                ', '.join(group['display_name'] for group in package['groups']),
                "https://data.humdata.org/dataset/{}".format(package['name']),
            ])
        time.sleep(DELAY) # give HDX a short rest
    start += CHUNK_SIZE # next chunk, but first ...

# end
