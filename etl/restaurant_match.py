import json, collections, ast
from pprint import pprint

class FeedParser:
    
    def __init__(self):
        # Counters used to track number of unique restaurants in feed
        self.yelp_places = collections.Counter()
        self.fs_places = collections.Counter()
        self.parse_fs('lower_manhattan_dump_fs.txt')
        self.parse_yelp('businesses.json')

    # Method to return key values
    def get_attr(self, dictnry, key):
        try:
            return dictnry[key]
        except KeyError, e:
            print "Key error: ", str(e)

    def parse_yelp(self, filename):
        with open("yelp.csv","w") as yelp_csv:
            with open(filename) as file:
                err = 0
                for line in file:
                    try:
                        review = json.loads(line)
                        # yelp_csv.write(",".join([review['name'],review['id'],
                        #     str(review['rating']) + '\n']).encode('utf-8'))
                        self.yelp_places[review['name']] += 1
                    except ValueError, e:
                        err += 1
                        print str(e)
                print "There were %s errors parsing Yelp data" % (err)
        print self.yelp_places.most_common(20)

    def parse_fs(self, filename):
        self.data = []
        with open("fs.csv","w") as fs_csv:
            # Reads FourSquare feed and extracts elements to create Id map
            with open(filename) as file:
                err = 0
                for line in file:
                    try:
                        # Data stored as Python literals
                        row = ast.literal_eval(line)
                        # Get name and location info for venues
                        venue = self.get_attr(row, 'venue')
                        if not venue is None:
                            name = self.get_attr(venue, 'name')
                            self.fs_places[name] += 1
                
                    except ValueError, e:
                        err += 1
                        print str(e)
                print "There were %s errors parsing FourSquare data" % (err)

        print self.fs_places.most_common(20)
    
if __name__ == '__main__':
    parser = FeedParser()