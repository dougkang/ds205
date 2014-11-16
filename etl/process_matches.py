import pymongo
from restaurant_matcher import *

class ProcessMatches:
    
    def __init__(self,source):
        self.client = pymongo.MongoClient()
        self.db = self.client.yelpsquare
        self.collection = self.db.restaurants
        self.records = []
        self.cur = self.collection.find({"map":"", "source":source})
        self.flag = True
        
        while(self.flag):
            self.records = []
            for i in range(100):
                self.record = next(self.cur, None)
                if self.record:
                    self.records.append(self.record)
                else:
                    self.flag = False
                    break
            RestaurantMatcher(self.records)
            
if __name__ == '__main__':
    processor = ProcessMatches("foursquare")