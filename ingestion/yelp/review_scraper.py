import requests
from requests_oauthlib import OAuth1
import configparser
import argparse
import time
import json

if __name__ == '__main__':
  
  # Parse input arguments
  parser = argparse.ArgumentParser(description='Retrieves Yelp Reviews')
  parser.add_argument('--config', metavar='c', required=False, type=str, 
      default='./config.ini', help='path to config file')
  parser.add_argument('--businesses', metavar='z', required=False, type=str, 
      default='./businesses.json', help='path to businesses.json')
  parser.add_argument('--output', metavar='o', required=False, type=str, 
      default='./reviews.json', help='path to output file')
  args = parser.parse_args()

  # Read configs using config parser
  config = configparser.ConfigParser()
  config.read(args.config)
  app_key = config['Yelp']['AppKey']
  app_secret = config['Yelp']['AppSecret']
  token_key = config['Yelp']['TokenKey']
  token_secret = config['Yelp']['TokenSecret']
  crawler_wait = float(config['Yelp']['CrawlerWait'])
  # retrieve limit is maximum 20, so take any config value less than or equal to 20
  retrieve_limit = min(int(config['Yelp']['RetrieveLimit']), 20)
  max_retries = int(config['Yelp']['MaxRetries'])
  yelp_api = config['Yelp']['Url']
 
  with open(args.businesses, 'r') as f_businesses, open(args.output, 'w') as f_output:
    # Initialize all the things we need for scraping 
    auth = OAuth1(app_key, app_secret, token_key, token_secret)

    # Iterate through each of our businesses and retreive our data
    success = []
    failed = []
    n_results = 0
    print "Starting parse on businesses found in %s" % args.businesses
    for x in f_businesses:
      # sleep between API requests to make sure we don't hit the rate limit
      time.sleep(crawler_wait) 

      try:
        js = json.loads(x)
      except Exception as e:
        print "Skipping invalid json %s: %s" % (x, e)
        continue

      if 'id' not in js:
        print "Skipping json with missing id: %s" % js 
        continue 

      retries = 0
      while retries < max_retries:
        retries = retries + 1
        business_id = js['id']

        # Build the API call
        url = "%s/v2/business/%s" % (yelp_api, business_id)
        print "[%d]: %s -" % (n_results, url),

        # Make the API call and parse the json
        res = requests.get(url, auth = auth)

        if res.status_code == 200:
          n_results = n_results + 1
          print "success"
          f_output.write(res.text)
          success.append(business_id)
          break
        else:
          # if something failed, increment the max retries 
          print "FAILED %s (%d/%d) retries left" % (res.status_code, retries, max_retries)
          failed.append(business_id)

    # print some diagnostics once we run through the complete zip code set
    print "COMPLETED"
    print "%d success, %d failures" % (len(success), len(failed))
    print "failed: [%s]" % ",".join(failed)

