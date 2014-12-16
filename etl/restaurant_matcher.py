import pymongo,math,elasticsearch,sys


class RestaurantMatcher:
    
    def __init__(self, lookups):
      self.client = pymongo.MongoClient(
        host='ec2-54-173-129-226.compute-1.amazonaws.com', port=27017)
      self.client2 = pymongo.MongoClient()
      self.db = self.client.yelpsquare
      self.db2 = self.client2.yelpsquare
      self.collection = self.db2.restaurants
      # The destination collection is where output is stored
      self.destination = self.db.matched_restaurants
      self.es = elasticsearch.Elasticsearch()

      for lookup in lookups:
        match = self.find_match(lookup)
        
        if match is not None:
          mapped = match["_source"]["map"]
          if mapped is not None:   
            # If there is a match that doesn't have a mapped id
            if mapped == "":
              print "update record"
              self.update_records(lookup, match)
            # If there is a match that has a mapped id, need a tie break
            elif mapped != "":
              print "tie break"
              self.tie_break(lookup, match)
          else:
            print "Error: no map value"
        # If no match, put in destination as is
        else:
          record = {}
          if lookup["source"] == "foursquare":
            record["fs_id"] = lookup["_id"]
            record["yelp_id"] = ""
            self.destination.remove({"fs_id":lookup["_id"]},"true")
          else:
            record["yelp_id"] = lookup["_id"]
            record["fs_id"] = ""
            self.destination.remove({"yelp_id":lookup["_id"]},"true")
          record["city"] = lookup["city"]
          record["state"] = lookup["state"]
          record["addr"] = lookup["addr"]
          record["postalcode"] = lookup["postalcode"]
          record["lat"] = lookup["lat"]
          record["long"] = lookup["long"]
          self.destination.insert(record)
          lookup["parsed"] += 1
          self.collection.save(lookup)
          print 'unmapped', lookup["_id"]
         
    def find_match(self, record):
      """Query the Elasticsearch index and return best match."""
      # If missing lat, set to 0 to accommodate cosine function
      record["lat"] = 0 if record["lat"] == "" else record["lat"]
      record["long"] = 0 if record["long"] == "" else record["long"]
      
      # Find the ten best matches
      # Easy optimization, if address exists require it to match *************
      res = self.es.search(index='yelpsquare.restaurants', body = 
        { "query" : { "filtered": { "query" : {
             "bool": {
               "must": [
                 { "match": {"name": { 
                     "query": record["name"],
                     "fuzziness": 1 }}},
               ],
               "should": [
                 { "match": { "city" : record["city"]}},
                 { "match": { "state" : record["state"]}},
                 { "match": { "postalcode" : record["postalcode"]}},
                 { "match": { "addr" : record["addr"]}},
                 { "match": {
                     "addr_num": {
                       "query": record["addr_num"],
                       "boost": 5}}},
                 # Looks for a location within +- 250 ft, 
                 # approximately the same city block.
                 { "fuzzy": {
                     "lat": { 
                       "value": record["lat"],
                       "boost": 5, 
                       "fuzziness": (.001*250*9/55)}}},
                 { "fuzzy": {
                     "long": { 
                       "value": record["long"],
                       "boost": 5, 
                       "fuzziness": (.001*250*9/55* \
                         math.cos(math.radians(
                           record["lat"])))}}}
               ],
               "minimum_should_match":4
             }
           }, "filter": {
               "not": {
                   "term": {
                       "source": record["source"]
                   }
               }
           }
        }}}
      )
      scores = [result["_score"] for result in res['hits']['hits']]
      if res['hits']['hits']:
        result = res['hits']['hits'][0]
        # If the top two results have the same score, don't return any matches
        if len(scores) > 1:
          if scores[0] == scores[1]:
            return
        # If the top score has a score < 1, don't return any matches
        if scores[0] < 4.2:
          return
        print record
        print result
        return result
      return


    def update_records(self, lookup, match):
      """Update the MongoDB record with the match returned by Elasticsearch"""
      # Get result record from MongoDB
      match_record = self.collection.find_one({"_id":match["_id"]})
      # The mapped_record will be used to insert into destination collection
      mapped_record = {}
      
      # Delete record(s) from the output collection and set ids in mapped record
      if lookup["source"] == "yelp":
        self.destination.remove({"yelp_id":lookup["_id"]},"true")
        self.destination.remove({"fs_id":match["_id"]},"true")
        mapped_record["yelp_id"] = lookup["_id"]
        mapped_record["fs_id"] = match_record["_id"]
      else:
        self.destination.remove({"fs_id":lookup["_id"]},"true")
        self.destination.remove({"yelp_id":match["_id"]},"true")
        mapped_record["fs_id"] = lookup["_id"]
        mapped_record["yelp_id"] = match_record["_id"]

      # Combine data from both sources
      if lookup["addr"] == "":
        mapped_record["addr"] = match["_source"]["addr"]
      else:
        mapped_record["addr"] = lookup["addr"]
      
      if lookup["city"] == "":
        mapped_record["city"] = match["_source"]["city"]
      else:
        mapped_record["city"] = lookup["city"]
        
      if lookup["state"] == "":
        mapped_record["state"] = match["_source"]["state"]
      else:
        mapped_record["state"] = lookup["state"]
        
      if lookup["postalcode"] == "":
        mapped_record["postalcode"] = match["_source"]["postalcode"]
      else:
        mapped_record["postalcode"] = lookup["postalcode"]
        
      if lookup["lat"] == "":
        mapped_record["lat"] = match["_source"]["lat"]
        mapped_record["long"] = match["_source"]["long"]
      else:
        mapped_record["lat"] = lookup["lat"]
        mapped_record["long"] = lookup["long"]
      
      # Set the new map values in restaurants collection   
      lookup["map"] = match["_id"]
      lookup["map_score"] = match["_score"]
      lookup["parsed"] += 1
      match_record["map"] = lookup["_id"]
      match_record["map_score"] = match["_score"]
      
      # Update the records in MongoDB
      self.collection.update({"_id":lookup["_id"]}, {"$set": lookup}, 
        upsert=False)
      self.collection.update({"_id":match_record["_id"]}, 
        {"$set": match_record}, upsert=False)
      self.destination.insert(mapped_record)
        
    def tie_break(self, lookup, match):
      """Given a matched record that already has a mapped id, determine
      which is the better match and update the records accordingly."""
      try:
        old_lookup = self.collection.find_one({"_id":match["_source"]["map"]})
        match_record = self.collection.find_one({"_id":match["_id"]})
    
        # If the result is already mapped to the lookup, do nothing
        if match_record["map"] == lookup["_id"]:
          print "already matched"
          lookup["parsed"] += 1
          self.collection.save(lookup)
          return
        
        # Get new match on record previously identified as match
        if old_lookup is not None:
          old_match = self.find_match(old_lookup)
          if old_match is not None:
            if old_match["_id"] == match["_id"]:
              old_score = old_match["_score"]
              new_score = match["_score"]
              print 'old score is ', old_score
              print 'new score is ', new_score
              # If old match has a higher score, update current match
              if old_score > new_score:
                match_record["parsed"] += 1
                match_record["map"] = ""
                self.collection.save(match_record)
                print "old score greater than new score"
              # If new match has a higher score, update old match and new matches
              elif old_score < new_score:
                print "old score less than new score"
                old_lookup["map"] = ""
                old_lookup["parsed"] += 1
                self.collection.save(old_lookup)
                self.update_records(lookup, match)
              # If both matches have same score, update lookup record for 
              # troubleshooting purposes
              elif old_score == new_score:
                print 'tie!'
                lookup["matches"] = [(match["_id"],match["_score"]),
                                     (old_match["_id"],old_match["_score"])]
                old_lookup["map"] = ""
                lookup["map"] = ""
                lookup["parsed"] += 1
                self.collection.save(lookup)
                self.collection.save(old_lookup)
            else:
              print "old match not the same as new match"
              self.update_records(lookup, match)
          else:
            print "old match is none"
            self.update_records(lookup, match)
        else:
          print "old lookup is none"
      
      except KeyError as e:
        print "KeyError:", e


if __name__ == '__main__':
  # Argument should be a list of records to match
  matcher = RestaurantMatcher()

        