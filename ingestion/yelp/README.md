Yelp Scraper
===

Uses the Yelp API to extract various aspects of Yelp data.

## Restaurant Scraper

Given a list of zipcodes, uses the search api to find restaurants in each zipcode and writes the
json output to disk.

By default, ensures a 2s delay between requests.

```
usage: restaurant_scraper.py [-h] [--config c] [--zipcodes z] [--output o]

Retrieves Yelp Restaurants

optional arguments:
  -h, --help    show this help message and exit
  --config c    path to config file
  --zipcodes z  path to zipcode file
  --output o    path to output file
```
