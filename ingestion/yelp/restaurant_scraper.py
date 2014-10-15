import requests
from requests_oauthlib import OAuth1
import configparser
import argparse
import time
import json

if __name__ == '__main__':
  
  # Parse input arguments
  parser = argparse.ArgumentParser(description='Retrieves Yelp Restaurants')
  parser.add_argument('--config', metavar='c', required=False, type=str, 
      default='./config.ini', help='path to config file')
  parser.add_argument('--zipcodes', metavar='z', required=False, type=str, 
      default='./zipcodes.lst', help='path to zipcode file')
  parser.add_argument('--output', metavar='o', required=False, type=str, 
      default='./businesses.json', help='path to output file')
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
 
  with open(args.zipcodes, 'r') as f_zipcodes, open(args.output, 'w') as f_output:
    # Initialize all the things we need for scraping 
    auth = OAuth1(app_key, app_secret, token_key, token_secret)
    zipcodes = [ int(x) for x in f_zipcodes if len(x) > 0 ]
    category_filters = json.loads(config['Yelp']['CategoryFilters'])

    # Iterate through each of our zipcodes and retreive our data
    success = []
    failed = []
    n_zipcodes = len(zipcodes)
    print "Starting parse on %d zipcodes found in %s" % (n_zipcodes, args.zipcodes)
    for i, z in enumerate(zipcodes):
      retries = 0
      n_results = 0 
      total_results = -1 
      while (total_results < 0 or n_results < total_results) and retries < max_retries:
        offset = n_results

        # Build the API call
        url = "%s/v2/search?category_filter=%s&location=%d&offset=%d" % \
          (yelp_api, ",".join(category_filters), z, offset)
        print "[%d/%d] %d off: %d/%d: %s -" % (i, n_zipcodes, z, offset, total_results, url),

        # Make the API call and parse the json
        res = requests.get(url, auth = auth)
        try:
          js = json.loads(res.text)
        except Exception as e:
          res.status_code == None

        if res.status_code == 200:
          n_results = n_results + retrieve_limit 
          retries = 0
          print "success"
          # one thing that I noticed is that the total changes depending on which server we
          # eventually hit. In order to make sure we grab everything, get the max total and
          # always just use that 
          total_results = max(int(js["total"]), total_results)
          f_output.write("\n".join([json.dumps(x) for x in js["businesses"]]) + "\n")
          success.append(str((z, offset)))
        else:
          # if something failed, increment the max retries 
          print "FAILED %s (%d/%d) retries left" % (res.status_code, retries, max_retries)
          retries = retries + 1
          failed.append((str(z), offset))

        # sleep between API requests to make sure we don't hit the rate limit
        time.sleep(crawler_wait) 

    # print some diagnostics once we run through the complete zip code set
    print "COMPLETED"
    print "%d success, %d failures" % (len(success), len(failed))
    print "failed: [%s]" % ",".join(failed)

