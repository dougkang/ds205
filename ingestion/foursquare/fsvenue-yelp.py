import requests
from requests_oauthlib import OAuth1
import configparser
import argparse
import time
import json
import foursquare
import codecs

if __name__ == '__main__':
  
  # Parse input arguments
  parser = argparse.ArgumentParser(description='Retrieves Foursquare Venues from Yelp Businesses')
  parser.add_argument('--config', metavar='c', required=False, type=str, 
      default='./config.ini', help='path to config file')
  parser.add_argument('--dataset', metavar='b', required=False, type=str, 
      default='./yelp_businesses.json', help='path to yelp businesses json')
  parser.add_argument('--output', metavar='o', required=False, type=str, 
      default='./dump_fs.txt', help='path to output file')
  args = parser.parse_args()

  # Read configs using config parser
  config = configparser.ConfigParser()
  config.read(args.config)
  c_id = config['Foursquare']['ClientId']
  c_secret = config['Foursquare']['ClientSecret']
  crawler_wait = float(config['Foursquare']['CrawlerWait'])
 
  with codecs.open(args.dataset, 'r', 'utf-8') as f_dataset, codecs.open(args.output, 'w', 'utf-8') as f_output:
    # Initialize all the things we need for scraping 
    client = foursquare.Foursquare(client_id = c_id, client_secret = c_secret)

    # Iterate through each of our objets in the academic dataset and retreive our data
    failed = []
    n_results = 0
    print "Starting parse on businesses found in %s" % args.dataset
    for x in f_dataset:

      try:
        js = json.loads(x)
      except Exception as e:
        print "Skipping invalid json %s: %s" % (x, e)
        continue

      # sleep between API requests to make sure we don't hit the rate limit
      time.sleep(crawler_wait) 

      name = js['name'].lower()
      latitude = js['location']['coordinate']['latitude']
      longitude = js['location']['coordinate']['longitude']
      yelpid = js['id']

      print "[%d]: %s,%s:(%f,%f) -" % (n_results, name, yelpid, latitude, longitude),
      n_results = n_results + 1

      try:
        address = js['location']['address']
        phone = js['display_phone']
        result = client.venues.search(params = { 
          "name": name, "ll": "%f,%f" % (latitude, longitude), "address": address, "phone": phone, "intent": "match" })
        fsid = result["venues"][0]["id"]
        # sleep between API requests to make sure we don't hit the rate limit
        time.sleep(crawler_wait) 
        venue = client.venues(fsid)
        venue["venue"]["yelpid"] = yelpid
        print "success"
        f_output.write(json.dumps(venue, ensure_ascii = False) + "\n")
      except Exception as e:
        # if something failed, increment the max retries 
        print "FAILED %s" % (e)
        failed.append(yelpid)

    # print some diagnostics once we run through the complete zip code set
    print "COMPLETED"
    print "%d success, %d failures" % (n_results - len(failed), len(failed))
    print "failed: [%s]" % ",".join(failed)
