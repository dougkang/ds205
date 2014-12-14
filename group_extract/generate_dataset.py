import json
import argparse
import configparser
from pymongo import MongoClient
from bson import json_util
import csv
import sys
import codecs

reload(sys)
sys.setdefaultencoding('UTF8')

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
  parser.add_argument('--output', metavar='o', required=False, type=str, 
      default='./data.csv', help='path to output file')
  args = parser.parse_args()

  # Read configs using config parser
  config = configparser.ConfigParser()
  config.read(args.config)
  mongo_host = config['Mongo']['Host']
  mongo_port = int(config['Mongo']['Port'])
  db_name = config['Yelpsquare']['FeatureDB']
  coll_name = config['Yelpsquare']['FeatureCollection']

  try:
    connection = MongoClient(mongo_host, mongo_port, safe=True)
    collection = connection[db_name][coll_name]
  except Exception as e:
    print "Something went wrong with the DB connection!: %s" % e
    exit(1)

  with codecs.open(args.output, 'w', 'utf-8') as f_output:
    print "Dumping features to disk"
    writer = csv.writer(f_output, delimiter=',', quotechar='"')
    keys = [ "price_tier", "city", "name", "yelpid", "state", "street", 
             "categories", "no_tips", "no_males", "no_females" ]
    f_output.write(",".join(keys) + "\n")
    for x in collection.find({ "fsid": { "$exists": True }, "yelpid": { "$exists": True } }):
      try:
        v = []
        for k in keys:
          if type(x[k]) is list:
	    v.append("|".join(x[k]))
          else:
	    v.append(x[k])
        writer.writerow(v)
      except Exception as e:
        print e
        continue

  connection.close()

