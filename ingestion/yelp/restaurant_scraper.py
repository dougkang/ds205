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
      default='./out.json', help='path to output file')
  args = parser.parse_args()

  # Read configs using config parser
  config = configparser.ConfigParser()
  config.read(args.config)
  app_key = config['Yelp']['AppKey']
  app_secret = config['Yelp']['AppSecret']
  token_key = config['Yelp']['TokenKey']
  token_secret = config['Yelp']['TokenSecret']
  crawler_wait = float(config['Yelp']['CrawlerWait'])

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
      url = "%s/v2/search?category_filters=%s&location=%d" % \
        (yelp_api, ",".join(category_filters), z)
      print "[%d/%d] %d: %s -" % (i, n_zipcodes, z, url),

      res = requests.get(url, auth = auth)
      if res.status_code == 200:
        print "success"
        f_output.write(res.text)
        success.append(str(z))
      else:
        print "FAILED %s" % res.status_code
        failed.append(str(z))

      time.sleep(crawler_wait) 

    print "COMPLETED"
    print "%d success, %d failures" % (len(success), len(failed))
    print "failed: [%s]" % ",".join(failed)

