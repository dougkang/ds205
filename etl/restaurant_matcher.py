import pymongo,math,elasticsearch


class RestaurantMatcher:
    
    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client.yelpsquare
        self.collection = self.db.restaurants
        self.es = elasticsearch.Elasticsearch()

    def find_match(self, record):
        """Query the Elasticsearch index and return best match."""
        # If missing lat, set to 0 to accommodate cosine function
        record["lat"] = 0 if record["lat"] == "" else record["lat"]
        # Find the ten best matches 
        res = self.es.search(index='yelpsquare.restaurants',
                        body = { "query" : { "filtered": { "query" : {
                                   "bool": {
                                     "must": [
                                       { "fuzzy": {"name": record["name"]}},
                                     ],
                                     "must_not": [
                                       { "match" : {
                                           "source": record["source"]}}
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
                                       { "match": { 
                                           "addr_num": { 
                                             "query": record["addr_num"],
                                             "boost": 10}}},
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
        result = res['hits']['hits'][0]
        if scores[0] == scores[1]:
            return
        return result
        

if __name__ == '__main__':
    matcher = RestaurantMatcher()
    lookup = { "_id" : "4b0f6683f964a520066223e3", "addr_num" : "2450", "country" : "United States", 
               "state" : "CA", "addr" : "2450 Junipero Serra Blvd", "city" : "Daly City", 
               "long" : -122.47146106244402, "name" : "McDonald's", "lat" : 37.69206004571632, 
               "map" : "","postalcode" : "94015", "source" : "yelp" }
    result = matcher.find_match(lookup)
    print(result)