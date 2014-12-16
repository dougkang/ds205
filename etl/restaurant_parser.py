import json, collections, ast, pymongo, os, codecs, string

class RestaurantParser:
    
    def __init__(self):
        # Counters used to track number of unique restaurants in feed
        self.yelp_places = collections.Counter()
        self.fs_places = collections.Counter()
        # Need to update for AWS 
        self.client = pymongo.MongoClient(
          host='ec2-54-173-129-226.compute-1.amazonaws.com', port=27017)
        self.db = self.client.yelpsquare
        self.docs = self.db.restaurants
        
        # Process files from Yelp
        # Needs to point at s3, which would eliminate the file tree traversal
        for fn in os.listdir(os.path.realpath('yelp')): 
            filename, extension = os.path.splitext('yelp/' + fn)
            if extension == ".json":
                self.parse_yelp(filename + extension)
        
        # Process files from FourSquare
        # Needs to point at s3, which would eliminate the file tree traversal
        for fn in os.listdir(os.path.realpath('foursquare')):
            filename, extension = os.path.splitext('foursquare/' + fn)
            if extension == ".txt":
                self.parse_fs(filename + extension)
                

    def get_attr(self, dictnry, key):
        """Checks a dictionary for a key and returns it."""
        try:
            return dictnry[key]
        except KeyError as e:
            print("Key error: ", str(e))
            return ''
            
    def save_data(self, iden, data):
        """Saves data to a local MongoDB. If the id already exists,
           the document is updated."""
        check = self.docs.find_one({"_id": iden})
        if check is None:
            self.docs.insert(data)
        else:
            self.docs.save(data)
    
    def get_address_number(self, address):
        """Finds the first numeric word in the address and returns it.
        This does not accommodate address numbers that end with a letter."""
        if isinstance(address, list):
            for item in address:
                for i in item.split(' '):
                    if i.lower() == "ste":
                        break
                    if str.isdecimal(i):
                        return i
            return ''
        for item in address.split(' '):
            if item.lower() == "ste" or item.lower() == "suite":
                return ''
            if self.is_number(item):
                return item
        return ''            

    def is_number(self, s):
        try:
            float(s)
            return True
        except ValueError:
            return False
    
    def parse_yelp(self, filename):
        with open(filename) as file:
            err = 0
            for line in file:
                city = postalcode = state = country = address = ''
                latitude = longitude = addr_num = ''
                try:
                    # Data stored as json string
                    row = json.loads(line)
                    
                    # Get name and id
                    name = self.get_attr(row, 'name')
                    name = (x for x in name if 0 < ord(x) < 127)
                    name = ''.join(name)
                    yelp_id = self.get_attr(row, 'id')
                    yelp_id = (x for x in yelp_id if 0 < ord(x) < 127)
                    yelp_id = ''.join(yelp_id)
                    # Counter used to get quick overview
                    self.yelp_places[name] += 1
                    
                    # Get address info
                    location = self.get_attr(row, 'location')
                    if location != "":
                        city = self.get_attr(location, 'city')
                        # city = (x for x in city if 0 < ord(x) < 127)
                        # city = ''.join(city)
                        postalcode = self.get_attr(location, 'postal_code')
                        state = self.get_attr(location, 'state_code')
                        country = self.get_attr(location, 'country_code')
                        address = self.get_attr(location, 'address')
                        if address != "":
                            address =  ' '.join(address)
                        addr_num = self.get_address_number(address)
                        coord = self.get_attr(location, 'coordinate')
                        if coord != "":
                            latitude = self.get_attr(coord, 'latitude')
                            longitude = self.get_attr(coord, 'longitude')
                    
                    # Create dictionary and add to MongoDB
                    doc = {"_id":yelp_id, "name": name,
                            "city":city, "state":state, "postalcode":postalcode,
                            "country":country, "lat":latitude, "long":longitude,
                            "source":"yelp", "addr":address, "map":"", 
                            "addr_num":addr_num}
                    self.save_data(yelp_id, doc)
                    
                except ValueError as e:
                    err += 1
                    print("Value error:",str(e))
            print("There were %s errors parsing Yelp data" % (err))

    def parse_fs(self, filename):
        # Reads FourSquare feed and extracts elements to create Id map
        with codecs.open(filename, 'r', encoding='utf-8') as file:
            city = postalcode = state = country = address = ''
            latitude = longitude = addr_num = ''
            err = 0
            for line in file:
                try:
                    # Data stored as Python literals
                    row = ast.literal_eval(line)
                    
                    # Get name and id for venue
                    venue = self.get_attr(row, 'venue')
                    if venue != "":
                        name = self.get_attr(venue, 'name')
                        #name = str(name.encode('ascii', errors='ignore'))
                        name = (x for x in name if 0 < ord(x) < 127)
                        name = ''.join(name)
                        fs_id = self.get_attr(venue, 'id')
                        
                        # Counter used to get quick overview
                        self.fs_places[name] += 1                        
                        
                        # Get address info
                        location = self.get_attr(venue, 'location')
                        if location != "":
                            address = self.get_attr(location, 'address')
                            addr_num = self.get_address_number(address)
                            city = self.get_attr(location, 'city')
                            city = (x for x in city if 0 < ord(x) < 127)
                            city = ''.join(city)
                            state = self.get_attr(location, 'state')
                            postalcode = self.get_attr(location, 'postalCode')
                            country = self.get_attr(location, 'country')
                            latitude = self.get_attr(location, 'lat')
                            longitude = self.get_attr(location, 'lng')
                    
                    # Create dictionary and add to MongoDB
                    doc = {"_id":fs_id, "name": name, 
                           "city":city, "state":state, "postalcode":postalcode,
                           "country":country, "lat":latitude, "long":longitude,
                           "source":"foursquare", "addr":address, "map":"",
                           "addr_num":addr_num}
                    self.save_data(fs_id, doc)
                        
                except ValueError as e:
                    err += 1
                    print(str(e))
            print("There were %s errors parsing FourSquare data" % (err))
            print(self.yelp_places.most_common(20))
            print(self.fs_places.most_common(20))
            
if __name__ == '__main__':
    parser = RestaurantParser()
            