"""Crawl HDX to find datasets matching a specific CKAN tag and content keywords.
CKAN API documentation: http://docs.ckan.org/en/latest/api/
Python CKAN library: https://github.com/ckan/ckanapi

Started by David Megginson, 2018-03-01
"""

import ckanapi, time, sys, csv, requests, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


#
# Edit these parameters for your search (will be command-line options in the future)
#

DEFAULT_CKAN_TAGS = '3w'
"""CKAN tag to use to filter datasets before content search."""

DEFAULT_CONTENT_KEYWORDS = (
    b'Education',
    b'education',
)
"""Keywords to search in content (must be binary strings)."""


#
# Other constants (shouldn't need to change)
#

DELAY = 1
"""Time delay in seconds between datasets, to give HDX a break."""

CHUNK_SIZE=100
"""Number of datasets to read at once"""

CKAN_URL = 'https://data.humdata.org'
"""Base URL for the CKAN instance."""


#
# Functions
#

def main(ckan_tags, content_keywords, output_stream):
    """Main routine to run search."""

    def scan_for_keyword(package):
        """Scan all binary resources in a dataset for a binary keyword"""
        for resource in package['resources']:
            logger.debug('Processing resource %s', resource['name'])
            with requests.get(resource['url']) as result:
                for keyword in content_keywords:
                    if result.content.find(keyword) > -1:
                        return True
        return False

    # Open a connection to HDX
    ckan = ckanapi.RemoteCKAN(CKAN_URL)

    # Open a CSV output stream
    output = csv.writer(output_stream)

    output.writerow([
        'Locations',
        'Provider',
        'Dataset',
        'HDX link'
    ])

    # Iterate through all the datasets ("packages") and resources on HDX
    start = 0
    result_count = 999999 # just a big, big number; will reset on first search result
    while start < result_count:
        result = ckan.action.package_search(fq='tags:{}'.format(ckan_tags), start=start, rows=CHUNK_SIZE)
        result_count = result['count']
        logger.info("Read %d package(s)...", len(result['results']))
        for package in result['results']:
            logger.debug('Processing package %s', package['name'])
            if scan_for_keyword(package):
                output.writerow([
                    ', '.join(group['display_name'] for group in package['groups']),
                    package['organization']['title'],
                    package['title'],
                    "https://data.humdata.org/dataset/{}".format(package['name']),
                ])
            time.sleep(DELAY) # give HDX a short rest
        start += CHUNK_SIZE # next chunk, but first ...


#
# Command-line script
#
if __name__ == '__main__':
    main(ckan_tags=DEFAULT_CKAN_TAGS, content_keywords=DEFAULT_CONTENT_KEYWORDS, output_stream=sys.stdout)

# end
