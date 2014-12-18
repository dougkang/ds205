import json
import argparse
import configparser
from pymongo import MongoClient
from bson import json_util

def extract_restaurant_features(json):
  """Given a json object representing the restaurant, extract our features"""
  return {
    "rating": json["rating"] if "rating" in json else 0.0,
    "review_count": json["review_count"] if "review_count" in json else 0 }

if __name__ == '__main__':
  # Parse input arguments
  parser = argparse.ArgumentParser(description='Extracts Foursquare features from ingestion results')
  parser.add_argument('--config', metavar='c', required=False, type=str, 
      default='./config.ini', help='path to config file')
  parser.add_argument('--businesses', metavar='b', required=False, type=str, 
      default='./yelp_businesses.txt', help='path to dump_fs.txt')
  parser.add_argument('--output', metavar='o', required=False, type=str, 
      default='./yelp_features.json', help='path to output file')
  parser.add_argument('--dump', action="store_true",
      default=True , help='whether or not to dump to output file')
  parser.add_argument('--clear_db', action="store_true",
      default=False, help='whether or not to clear db before starting')
  args = parser.parse_args()

  # Read configs using config parser
  config = configparser.ConfigParser()
  config.read(args.config)
  mongo_host = config['Mongo']['Host']
  mongo_port = int(config['Mongo']['Port'])
  db_name = config['Yelpsquare']['FeatureDB']
  coll_name = config['Yelpsquare']['FeatureCollection']

  try:
    connection = MongoClient(mongo_host, mongo_port, safe = False)
    collection = connection[db_name][coll_name]
  except Exception as e:
    print "Something went wrong with the DB connection!: %s" % e
    exit(1)

  if args.clear_db:
    print "Clearing db"
    collection.drop()
 
  collection.ensure_index("yelpid")

  b_count = 0
  b_fail_count = 0
  with open(args.businesses, 'r') as f_businesses:
    print "Extracting businesses"
    for x in f_businesses:
      b_count = b_count + 1
      try:
        bjson = json.loads(x)
        vid = bjson["id"]
        print "[%d] %s: extracting restaurant features:" % (b_count, vid),
        feat = extract_restaurant_features(bjson)
        feat["yelpid"] = vid
        collection.save(feat)
        print "SUCCESS"
      except Exception as e:
        b_fail_count = b_fail_count + 1
        print "FAILED: %s" % e

  if args.dump:
    with open(args.output, 'w') as f_output:
      print "Dumping features to disk"
      for x in collection.find():
        f_output.write(json_util.dumps(x) + "\n")

  connection.close()
  print "COMPLETED: %d/%d businesses loaded" % (b_count - b_fail_count, b_count)
