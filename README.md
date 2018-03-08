Search HDX for datasets based on CKAN tags and resource content.

This type of search is not yet possible within CKAN itself, because CKAN search looks only at metadata, not content.

The script retrieves all resources matching a CKAN tag, then downloads the resources and does a binary search for a list of binary strings (note that this could get complicated with Unicode accented characters, etc).

The script is not yet generalised, so to change the search parameters, you will need to edit the variables DEFAULT_CKAN_TAGS and DEFAULT_CONTENT_KEYWORDS near the beginning (see comments in the source).

Requirements:

- Python3
- ckanapi package

Usage:

    python3 hdx-content-search.py > results.csv


This script is released into the Public Domain.
David Megginson, 2018-03-01