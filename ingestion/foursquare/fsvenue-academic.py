import requests
from requests_oauthlib import OAuth1
import configparser
import argparse
import time
import json
import foursquare
import codecs
import sys

reload(sys)
sys.setdefaultencoding('UTF8')

if __name__ == '__main__':
  
  # Parse input arguments
  parser = argparse.ArgumentParser(description='Retrieves Foursquare Venues from Yelp Academic Dataset')
  parser.add_argument('--config', metavar='c', required=False, type=str, 
      default='./config.ini', help='path to config file')
  parser.add_argument('--dataset', metavar='b', required=False, type=str, 
      default='./yelp_academic_dataset.json', help='path to yelp academic dataset')
  parser.add_argument('--output', metavar='o', required=False, type=str, 
      default='./dump_fs.txt', help='path to output file')
  args = parser.parse_args()

  # Read configs using config parser
  config = configparser.ConfigParser()
  config.read(args.config)
  c_id = config['Foursquare']['ClientId']
  c_secret = config['Foursquare']['ClientSecret']
  crawler_wait = float(config['Foursquare']['CrawlerWait'])
 
  with codecs.open(args.dataset, 'r', 'utf-8') as f_dataset, codecs.open(args.output, 'w', "utf-8") as f_output:
    # Initialize all the things we need for scraping 
    client = foursquare.Foursquare(client_id = c_id, client_secret = c_secret)

    # Iterate through each of our objets in the academic dataset and retreive our data
    failed = []
    n_results = 0
    print "Starting parse on businesses found in %s" % args.dataset
    for x in f_dataset:

      try:
        js = json.loads(x, encoding='utf-8')
      except Exception as e:
        print "Skipping invalid json %s: %s" % (x, e)
        continue
      
      if js["type"] != "business" or "categories" not in js or "Food" not in js["categories"]:
        print "Skipping non-business object"
        continue

      # sleep between API requests to make sure we don't hit the rate limit
      time.sleep(crawler_wait) 

      name = js['name'].lower()
      latitude = js['latitude']
      longitude = js['longitude']
      yelpid = js['business_id']
      city = js['city']
      address = js['full_address'].split("\n")[0]

      print "[%d]: %s,%s -" % (n_results, name, yelpid),
      n_results = n_results + 1

      try:
        address = js['full_address'].split('\n')[0]
        result = client.venues.search(params = { 
          "name": name, "ll": "%f,%f" % (latitude, longitude), "address": address, "intent": "match" })
        fsid = result["venues"][0]["id"]
        # sleep between API requests to make sure we don't hit the rate limit
        time.sleep(crawler_wait) 
        venue = client.venues(fsid)
        venue["yelpid"] = yelpid
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
