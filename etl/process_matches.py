import pymongo
from restaurant_matcher import *

class ProcessMatches:
    
    def __init__(self):
        self.client = pymongo.MongoClient()
          #host='ec2-54-173-129-226.compute-1.amazonaws.com', port=27017)
        self.db = self.client.yelpsquare
        self.collection = self.db.restaurants
        self.records = []
            
        while(True):
          self.cur = self.collection.find({"parsed":0}).limit(100)
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
            # Can optimize this by running multiple threads
            print self.records
            RestaurantMatcher(self.records)
          cur = self.collection.find({"parsed":0}).limit(1)
          if cur.count() == 0:
            break
            
if __name__ == '__main__':
    processor = ProcessMatches()