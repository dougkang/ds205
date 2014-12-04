import foursquare
import pprint
import pymongo
import sys
import time
import json

CLIENT_ID = "43RBUOR31LOK00MUTZGKKKWAAV4SMJBMUTKZDVB5WCV5TIIW"
CLIENT_SECRET = "IA1AT4VNTVLREUNAGDXDRQ251T3C4B1QQ0SVKUDDIKKVH4OE"

MENU_FILE = "menu_fs.txt"

class FSMenuCrawl:
    def __init__(self, menu_file):
        try:
            self.fsclient = foursquare.Foursquare(client_id = CLIENT_ID,
                                                  client_secret = CLIENT_SECRET)
        except:
            print "Could not connect to FourSquare API....Exiting!"
            exit(1)

        try:
            self.menuf = open(menu_file, "ab+")
        except:
            print "Could not open needed file(s)...Exiting!"
            exit(1)

        try:
            self.mclient = pymongo.MongoClient("mongodb://localhost",
                                                  w = 1, j = True)
        except:
            print "Something went wrong with the DB connection!"
            exit(1)

        try:
            self.db = self.mclient.fsquare
            self.fsquare = self.db.fsquare
        except:
            print "Something went wrong with using fsquare DB!"
            exit(1)

        try:
            self.fsmenu = self.db.fsmenu
        except:
            print "Something went wrong with using fsmenu Collection!"
            exit(1)

    def close(self):
        self.menuf.close()
        self.mclient.close()

    def insert(self, entry):
        try:
            print "[insert] " + str(entry)
            self.fsmenu.insert(entry)
        except pymongo.errors.DuplicateKeyError:
            pass
        except:
            print "DB insert failed on:"
            print entry

    def menu_write(self, text):
        self.menuf.write(text)

    def db_get_venues_with_menu(self):
        return self.fsquare.find({'vMenuURL': {'$exists': 1}})

    def get_menu(self, venue):
        return self.fsclient.venues.menu(venue, params = {})

def crawl_fs_menus(fsmenu):
    cursor = fsmenu.db_get_venues_with_menu() 
    count = 0
    for entry in cursor:
        print "[fsmenu] " + str(entry)
        menu = fsmenu.get_menu(entry['_id'])
        menu['id'] = entry['_id']
        fsmenu.menu_write(json.dumps(menu))
        fsmenu.menu_write("\n")
        time.sleep(0.75)

fsmenu = FSMenuCrawl(MENU_FILE)
crawl_fs_menus(fsmenu)
fsmenu.close()
