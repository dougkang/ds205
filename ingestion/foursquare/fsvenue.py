import math
import foursquare
import pprint
import pymongo
import sys
import time
import json

CLIENT_ID = "43RBUOR31LOK00MUTZGKKKWAAV4SMJBMUTKZDVB5WCV5TIIW"
CLIENT_SECRET = "IA1AT4VNTVLREUNAGDXDRQ251T3C4B1QQ0SVKUDDIKKVH4OE"

client = foursquare.Foursquare(client_id = CLIENT_ID,
                               client_secret = CLIENT_SECRET)

DUMP_FILE = "dump_fs.txt"
MILE_LEN = 1609.0
GRID_LEN = 400.0 # In meters
SCAN_RADIUS = round(GRID_LEN * math.sqrt(2) / 2)

MILES_PER_LAT = 69.0
MILES_PER_LONG = 47.0 # Just using an average

LAT_CHANGE = (GRID_LEN / MILE_LEN) / MILES_PER_LAT
LONG_CHANGE = (GRID_LEN / MILE_LEN) / MILES_PER_LONG

class FSCrawl:
    def __init__(self, dump_file):
        try:
            self.dumpf = open(dump_file, "ab+")
        except:
            print "Could not open needed file(s)...Exiting!"
            eixt(1)

        try:
            self.connection = pymongo.Connection("mongodb://localhost",
                                                 safe=True)
        except:
            print "Something went wrong with the DB connection!"
            exit(1)

        try:
            self.db = self.connection.fsquare
            self.fsquare = self.db.fsquare
        except:
            print "Something went wrong with using fsquare DB!"
            exit(1)

    def close(self):
        self.dumpf.close()
        self.connection.close()

    def insert(self, entry):
        try:
            self.fsquare.insert(entry)
        except pymongo.errors.DuplicateKeyError:
            pass
        except:
            print "DB insert failed on:"
            print entry

    def dump_write(self, text):
        self.dumpf.write(text)

def db_fetch_fsquare_venues(fscrawl, section, ll, radius):
    myvenues = client.venues.explore(params = {'section': section,
                                               'radius': radius,
                                               'limit': 50, 'll': ll,
                                               'llAcc': 50})
    for venue in myvenues['groups'][0]['items']:
        fscrawl.dump_write(json.dumps(venue))
        fscrawl.dump_write("\n")

        venue_entry = {}
        if 'venue' not in venue:
            continue
        if 'name' in venue['venue']:
            venue_entry['vName'] = venue['venue']['name'].encode('ascii',
                                                          errors = 'ignore')
        if 'id' in venue['venue']:
            venue_entry['_id'] = venue['venue']['id'].encode('ascii',
                                                      errors = 'ignore')
        if 'url' in venue['venue']:
            venue_entry['vURL'] = venue['venue']['url'].encode('ascii',
                                                        errors = 'ignore')
        if 'menu' in venue['venue'] and 'url' in venue['venue']['menu']:
            venue_entry['vMenuURL'] = \
                venue['venue']['menu']['url'].encode('ascii', errors = 'ignore')
        if 'location' in venue['venue']:
            if 'formattedAddress' in venue['venue']['location']:
                address_str = ""
                for field in venue['venue']['location']['formattedAddress']:
                    address_str += (field.encode('ascii', errors = 'ignore'))
                    address_str += " "
                venue_entry['vaddress'] = address_str
            if 'postalCode' in venue['venue']['location']:
                venue_entry['vZipcode'] = \
                    venue['venue']['location']['postalCode'].encode('ascii',
                                                             errors = 'ignore')

        # print venue_entry
        fscrawl.insert(venue_entry)

def crawl_fs_venues(fscrawl):
    sections = ['food', 'drinks', 'coffee']
    blat, blong = 38, -123
    elat, elong = 36, -121

    lat = blat
    long = blong
    while lat > elat:
        while long < elong:
            ll = str(lat) + "," + str(long)
            print "Gathering data for (lat, long): ", ll
            for section in sections:
                db_fetch_fsquare_venues(fscrawl, section, ll, SCAN_RADIUS)
                time.sleep(0.75)
            long += LONG_CHANGE
        lat -= LAT_CHANGE
        long = blong

fscrawl = FSCrawl(DUMP_FILE)
crawl_fs_venues(fscrawl)
fscrawl.close()
