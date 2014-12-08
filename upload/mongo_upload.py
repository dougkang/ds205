from pymongo import MongoClient
import argparse
import configparser
import json

if __name__ == '__main__':
  
  # Parse input arguments
  parser = argparse.ArgumentParser(description='Uploads results to mongo')
  parser.add_argument('--config', metavar='c', required=False, type=str, 
      default='./config.ini', help='path to config file')
  parser.add_argument('--yelp_businesses', metavar='y', required=False, type=str, 
      help='path to yelp busiensses')
  parser.add_argument('--fs_businesses', metavar='y', required=False, type=str, 
      help='path to foursquare businesses')
  parser.add_argument('--group_results', metavar='y', required=False, type=str, 
      default='./group_extract.json', help='path to group extraction')
  parser.add_argument('--menu_results', metavar='y', required=False, type=str, 
      default='./menu_mentions.json', help='path to menu mention ranking')
  args = parser.parse_args()

  # Read configs using config parser
  config = configparser.ConfigParser()
  config.read(args.config)
  host = config['Mongo']['Host']
  port = int(config['Mongo']['Port'])
  db = config['Yelpsquare']['DB']
  collection = config['Yelpsquare']['RestaurantsCollection']
  mongourl = "mongodb://%s:%d/" % (host, port)

  client = MongoClient(mongourl)
  mongo = client[db][collection]

  ybi_total = 0
  ybi_success = 0
  if args.yelp_businesses is not None:
    print "Pushing Yelp Businesses to Mongo..."
    with open(args.yelp_businesses, 'r') as f_businesses:
      for x in f_businesses:
        print "[%d]: " % (ybi_total),
        ybi_total = ybi_total + 1 
        try:
          js = json.loads(x)
          yelpid = js['id']
          print "%s -" % (yelpid),
          record = { "yelpid": yelpid,
                     "name": js['name'],
                     "city": js['location']['city'],
                     "state": js['location']['state_code'],
                     "addr": " ".join(js['location']['address']),
                     "phone": js['phone'] if 'phone' in js else None,
                     "rating": js['rating'],
                     "reviews": js['review_count'],
                     "yelpurl": js['url'],
                     "categories": [ x[0] for x in js['categories'] ],
                     "lat": js['location']['coordinate']['latitude'],
                     "long": js['location']['coordinate']['longitude'],
                     "hasyelp": True,
                   }
          mongo.update({ "yelpid": yelpid }, record, upsert = True)
          ybi_success = ybi_success + 1 
          print "success"
        except Exception as e:
          # if something failed, increment the max retries 
          print "FAILED %s" % (e)
  else:
    print "Skipping Yelp Businesses push"

  fsbi_total = 0
  fsbi_success = 0
  if args.fs_businesses is not None:
    print "Pushing Foursquare Businesses to Mongo..."
    with open(args.fs_businesses, 'r') as f_businesses:
      for x in f_businesses:
        print "[%d]: " % (fsbi_total),
        fsbi_total = fsbi_total + 1 
        try:
          js = json.loads(x)
          venue = js['venue']
          yelpid = venue['yelpid']
          print "%s -" % (yelpid),
          record = { "yelpid": yelpid,
                     "fsid": venue['id'],
                     "tips": venue['stats']['tipCount'],
                     "checkins": venue['stats']['checkinsCount'],
                     "hasfs": True,
                     "url": venue['url'] if 'url' in venue else None
                   }
          mongo.update({ "yelpid": yelpid }, record, upsert = True)
          fsbi_success = fsbi_success + 1 
          print "success"
        except Exception as e:
          # if something failed, increment the max retries 
          print "FAILED %s" % (e)
  else:
    print "Skipping Foursquare Businesses push"
  


  print "COMPLETED"
  print "- %d/%d ybi" % (ybi_success, ybi_total)
  print "- %d/%d fsbi" % (fsbi_success, fsbi_total)
