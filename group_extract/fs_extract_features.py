import json
import argparse
import configparser
from pymongo import MongoClient

def bag_of_words_attr(word, attr):
  return "%s:%s" % (word, attr)

def extract_restaurant_features(json):
  """Given a json object representing the restaurant, extract our features"""
  venue = json["venue"]
  price = venue["price"] if "price" in venue else {}
  location = venue["location"] if "location" in venue else {}
  address = location["address"].lower().split(" ", 1) if "address" in location else {}

  categories = [ 
      bag_of_words_attr(x["name"].lower(), "primary" if x["primary"] else "secondary") 
      for x in venue["categories"] ]

  return {
    "name": venue["name"].lower(),
    "price": price["message"].lower() if "message" in price else "unknown",
    "price_tier": price["tier"] if "tier" in price else 0,
    "state": location["state"].lower() if "state" in location else "unknown",
    "city": location["city"].lower() if "city" in location else "unknown",
    "house_no": address[0] if len(address) > 1 else "none",
    "street": address[-1] if len(address) > 0 else "unknown",
    "categories": categories }

def extract_tip_features(json):
  """Given a json object representing the tips, extract our features"""
  tips = json["tips"]["items"]

  female_count = 0
  male_count = 0
  text = []
  tip_count = 0
  for t in tips:
    tip_count = tip_count + 1
    if t["user"]["gender"] == "female":
      female_count = female_count + 1
    else:
      male_count = male_count + 1
    text.append(t["text"])

  return {
    "no_females": female_count,
    "no_males": male_count,
    "no_tips": tip_count,
    "tips": text }

if __name__ == '__main__':
  # Parse input arguments
  parser = argparse.ArgumentParser(description='Extracts Foursquare features from ingestion results')
  parser.add_argument('--config', metavar='c', required=False, type=str, 
      default='./config.ini', help='path to config file')
  parser.add_argument('--businesses', metavar='b', required=False, type=str, 
      default='./dump_fs.txt', help='path to dump_fs.txt')
  parser.add_argument('--tips', metavar='t', required=False, type=str, 
      default='./tips.json', help='path to tips.json')
  parser.add_argument('--output', metavar='o', required=False, type=str, 
      default='./fs_features.json', help='path to output file')
  parser.add_argument('--clear_db', metavar='x', nargs='?', type=bool, 
      default=False, help='whether or not to clear db before starting')
  args = parser.parse_args()

  # Read configs using config parser
  config = configparser.ConfigParser()
  config.read(args.config)
  mongo_host = config['Mongo']['Host']
  mongo_port = int(config['Mongo']['Port'])
  db_name = config['Foursquare']['FeatureDB']
  coll_name = config['Foursquare']['FeatureCollection']

  try:
    connection = MongoClient(mongo_host, mongo_port, safe=True)
    collection = connection[db_name][coll_name]
  except Exception as e:
    print "Something went wrong with the DB connection!: %s" % e
    exit(1)

  if args.clear_db:
    collection.drop()
 
  collection.ensure_index("id")

  b_count = 0
  b_fail_count = 0
  with open(args.businesses, 'r') as f_businesses:
    print "Extracting businesses"
    for x in f_businesses:
      b_count = b_count + 1
      try:
        bjson = json.loads(x)
        vid = bjson["venue"]["id"]
        print "[%d] %s: extracting restaurant features:" % (b_count, vid),
        feat = extract_restaurant_features(bjson)
        feat["id"] = vid
        collection.insert(feat)
        print "SUCCESS"
      except Exception as e:
        b_fail_count = b_fail_count + 1
        print "FAILED: %s" % e

  t_count = 0
  t_fail_count = 0
  with open(args.tips, 'r') as f_tips:
    print "Extracting tips"
    for x in f_tips:
      t_count = t_count + 1
      try:
        bjson = json.loads(x)
        vid = bjson["venue_id"]
        print "[%d] %s: extracting tip features:" % (t_count, vid),
        feat = extract_tip_features(bjson)
        collection.find_and_modify({ "id": vid }, { "$set": feat })
        print "SUCCESS"
      except Exception as e:
        t_fail_count = t_fail_count + 1
        print "FAILED: %s" % e

  connection.close()
  print "COMPLETED: %d/%d businesses loaded, %d/%d tips loaded" % (b_count - b_fail_count, b_count, t_count - t_fail_count, t_count)
