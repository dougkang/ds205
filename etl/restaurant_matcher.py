import pymongo,math,elasticsearch,sys


class RestaurantMatcher:
    
    def __init__(self, lookups):
        self.client = pymongo.MongoClient()
        self.db = self.client.yelpsquare
        self.collection = self.db.restaurants
        self.es = elasticsearch.Elasticsearch()

        # lookup = {'addr': ['115 De Anza Blvd'], 'state': 'CA', 'long': -122.33677346688,
        #     'lat':37.521342711976004, 'country': 'US', 'source': 'yelp', 'map': '', 'addr_num': '115',
        #     '_id':'wakuriya-san-mateo', 'name': 'Wakuriya', 'postalcode': '94402', 'city': 'San Mateo'}
        for lookup in lookups:
            match = self.find_match(lookup)
            if match is not None:
                mapped = match["_source"]["map"]

                # If there is a match that doesn't have a mapped id
                if mapped == "":
                    print("update record")
                    self.update_records(lookup, match)
                # If there is a match that has a mapped id, need a tie break
                elif mapped != "":
                    self.tie_break(lookup, match)
        
    # --------------> need to update to handle issue with yelp addr list    
    def find_match(self, record):
        """Query the Elasticsearch index and return best match."""
        # If missing lat, set to 0 to accommodate cosine function
        record["lat"] = 0 if record["lat"] == "" else record["lat"]
        record["long"] = 0 if record["long"] == "" else record["long"]
        # Find the ten best matches 
        res = self.es.search(index='yelpsquare.restaurants',
                        body = { "query" : { "filtered": { "query" : {
                                   "bool": {
                                     "must": [
                                       # Need to make sure first 2 characters match <------------!!!!!
                                       { "fuzzy": {"name": {"value":record["name"],
                                                            "prefix_length":2}}},
                                     ],
                                     "should": [
                                       { "match": { 
                                           "city" : record["city"]}},
                                       { "match": { 
                                           "state" : record["state"]}},
                                       { "match": { 
                                           "postalcode" : record["postalcode"]}},
                                       { "match": { 
                                           "addr" : record["addr"]}},
                                       # { "match": {
                                       #     "addr_num": {
                                       #       "query": record["addr_num"],
                                       #       "boost": 10}}},
                                       # Looks for a location within +- 250 ft. 
                                       # Approximately the same city block.
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
                                     "minimum_should_match":2
                                   }
                                 }, "filter": {
                                     "not": {
                                         "term": {
                                             "source": record["source"]
                                         }
                                     }
                                 }
                               }}})
        scores = [result["_score"] for result in res['hits']['hits']]
        if res['hits']['hits']:
            result = res['hits']['hits'][0]
            # Should we count top two as matches?
            if len(scores) > 1:
                if scores[0] == scores[1] or scores[0] < 2:
                    return
            return result
        return


    def update_records(self, lookup, match):
        """Update the MongoDB record with the match returned by Elasticsearch"""
        # Get result record from MongoDB
        match_record = self.collection.find_one({"_id":match["_id"]})

        # Set the new map values        
        lookup["map"] = match["_id"]
        match_record["map"] = lookup["_id"]
        
        # Update the records in MongoDB
        self.collection.update({"_id":lookup["_id"]}, {"$set": lookup}, 
            upsert=False)
        self.collection.update({"_id":match_record["_id"]}, 
            {"$set": match_record}, upsert=False)
        
    def tie_break(self, lookup, match):
        """Given a matched record that already has a mapped id, determine
        which is the better match and update the records accordingly."""
        try:
            # If the result is already mapped to the lookup, do nothing
            if match["map"] == lookup["_id"]:
                return 
            old_lookup = self.collection.find_one({"_id":match["map"]})
        
            # Get new match on record previously identified as match
            if old_lookup is not None:
                old_match = matcher.find_match(prev_lookup)
                if old_match["_id"] == match["_id"]:
                    old_score = old_match["_score"]
                    new_score = match["_score"]
                    # If old match has a higher score, do nothing
                    if old_score > new_score:
                        return
                    # If new match has a higher score, update prev match and result record maps
                    elif old_score < new_score:
                        old_lookup["map"] = ""
                        self.update_records(lookup, match)
                    elif old_score == new_score:
                        lookup["matches"] = [(match["_id"],match["_score"]),
                                             (old_match["_id"],old_match["_score"])]
        except KeyError as e:
            print(e)
            print(lookup)
            print(match)


if __name__ == '__main__':
    # Argument should be a list of records to match
    matcher = RestaurantMatcher()

        