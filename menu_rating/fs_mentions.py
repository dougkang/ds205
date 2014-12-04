from mrjob.job import MRJob
import sys
import string
import re
import json
import pickle
from itertools import groupby
from nltk.corpus import stopwords

NONASCII_RE = re.compile(r'[^\x00-\x7F]+')
PUNCTUATION_RE = re.compile('[%s]+' % re.escape(string.punctuation))
WORD_RE = re.compile(r"[\w']+")
STOPWORDS_SET = set(stopwords.words('english'))

def clean(text):
  '''Cleans text by replacing all non-ascii characters and punctuation with space and converting all characters to lowercase'''
  asciified = NONASCII_RE.sub(' ', text)
  rem_punct = PUNCTUATION_RE.sub(' ', asciified)
  return rem_punct.lower()

def get_menuitem(js):
  '''Recursively retrieves the menu item from a menu item json object'''
  if 'entryId' in js:
    return [ js['name'] ]
  elif 'entries' in js:
    return reduce(lambda x, y: x + y, [ get_menuitem(x) for x in js['entries']['items'] ],  [])
  elif 'items' in js:
    return reduce(lambda x, y: x + y, [ get_menuitem(x) for x in js['items'] ], [])
  elif 'menus' in js:
    return get_menuitem(js['menus'])
  elif 'menu' in js:
    return get_menuitem(js['menu'])
  else:
    return []

class ExtractTips(MRJob):
  '''Extracts tips from the raw json output that we get from tip ingestion'''

  def mapper(self, _, line):
    js = json.loads(line)
    tips = [ x["text"] for x in js['tips']['items'] ]
    venue_id = js['venue_id']
    for tip in tips:
      yield (venue_id, json.dumps((clean(tip), 0)))

class ExtractReviews(MRJob):
  '''Extracts reviews from the raw json output that we get from yelp review academic dataset'''

  def mapper(self, _, line):
    js = json.loads(line)
    # Since we want to appropriately punish food mentions that correspond with lower ratings,
    # assuming that if a food was mentioned in review with a bad rating, it was probably because
    # the food was bad, normalize the reviews by subtracting by 2.5.
    # This gives us the added benefit that even a neutral review of 3 will still contribute to the
    # ranking of the menu item
    rating = x["stars"] - 2.5
    review = { "text": clean(x["text"]), "rating": rating }
    venue_id = js['business_id']
    yield (venue_id, json.dumps((review, 0)))

class ExtractMenu(MRJob):
  '''Extracts menus from the raw json output that we get from tip ingestion'''

  def mapper(self, _, line):
    js = json.loads(line)
    venue_id = js['id']
    menuitems = get_menuitem(js)
    for mi in menuitems:
      yield (venue_id, json.dumps((clean(mi), 1)))

class AccumulateYelpRatings(MRJob):
  '''Does an inner join of all possible combinations of reviews and menus, then
     accumulates the scores of menuitems mentioned in the reviews'''

  def extract(self, _, line):
    [k, v] = line.split("\t")
    # Weird thing - since we our default output protocol wraps the fields in
    # quotes, we need to do a json load twice: first to load the result as
    # a json string, thereby removing the surrounding double quotes and
    # escape chars and then a second time to load the actual data
    yield (json.loads(k), json.loads(json.loads(v)))

  def join_menu_w_reviews(self, venue_id, vs):
    # Much of the work is being done at this reduce phase.  Some work could
    # be done to split this out into a number of mrjob steps
    groups = {} 
    sorted_vs = sorted(vs, key = lambda x: x[1])
    for t, g in groupby(sorted_vs, lambda x: x[1]):
      groups[t] = [ x[0] for x in g ]
    if 0 in groups and 1 in groups:
      for review in groups[0]:
        review_words = set(WORD_RE.findall(review["text"])) - STOPWORDS_SET
        for menuitem in groups[1]:
          menu_words = set(WORD_RE.findall(menuitem)) - STOPWORDS_SET
          # If any part of the menuitem was mentioned, then chalk it up as a mention
          num_mentions = len(menu_words.intersection(review_words))
          yield ("%s|%s" % (venue_id, menuitem), review["rating"] if num_mentions > 0 else 0)
    elif 1 in groups:
      for menuitem in groups[1]:
        # If we are at this point, this means that there were no reviews for this
        # restaurant.  We can't do anything, so spit out 0 for every entry
        yield ("%s|%s" % (venue_id, menuitem), 0)
  
  def sum(self, key, vs):
    yield (key, sum(vs))

  def steps(self):
    # This is less than ideal: we are doing these as two reduces, which
    # are expensive, resource-wise.  TODO figure out a better way to do this
    return [ 
      self.mr(mapper=self.extract, reducer=self.join_menu_w_reviews), 
      self.mr(reducer=self.sum) 
    ]

class AccumulateFoursquareMentions(MRJob):
  '''Does an inner join of all possible combinations of tips and menus, then
     counts the number of times a menu item was mentioned in a tip'''

  def extract(self, _, line):
    '''The data that we will be extracting will be of the form: %s\t[%s, %s], 
       so load it into memory in such a way we can handle it'''
    [k, v] = line.split("\t")
    # Weird thing - since we our default output protocol wraps the fields in
    # quotes, we need to do a json load twice: first to load the result as
    # a json string, thereby removing the surrounding double quotes and
    # escape chars and then a second time to load the actual data
    yield (json.loads(k), json.loads(json.loads(v)))

  def join_menu_w_tips(self, venue_id, vs):
    # Much of the work is being done at this reduce phase.  Some work could
    # be done to split this out into a number of mrjob steps
    groups = {} 
    sorted_vs = sorted(vs, key = lambda x: x[1])
    for t, g in groupby(sorted_vs, lambda x: x[1]):
      groups[t] = [ x[0] for x in g ]
    if 0 in groups and 1 in groups:
      for tip in groups[0]:
        tip_words = set(WORD_RE.findall(tip)) - STOPWORDS_SET
        for menuitem in groups[1]:
          menu_words = set(WORD_RE.findall(menuitem)) - STOPWORDS_SET
          # If any part of the menuitem was mentioned, then chalk it up as a mention
          num_mentions = len(menu_words.intersection(tip_words))
          yield ("%s|%s" % (venue_id, menuitem), 1 if num_mentions > 0 else 0)
    elif 1 in groups:
      for menuitem in groups[1]:
        # If we are at this point, this means that there were no tips for this
        # restaurant.  We can't do anything, so spit out 0 for every entry
        yield ("%s|%s" % (venue_id, menuitem), 0)
  
  def sum(self, key, vs):
    yield (key, sum(vs))

  def steps(self):
    # This is less than ideal: we are doing these as two reduces, which
    # are expensive, resource-wise.  TODO figure out a better way to do this
    return [ 
      self.mr(mapper=self.extract, reducer=self.join_menu_w_tips), 
      self.mr(reducer=self.sum) 
    ]

if __name__ == '__main__':
   if len(sys.argv) <= 1:
     print >> sys.stderr, "Need to specify job to run [menu, tips, reviews, fs-mentions, yelp-mentions]"
     sys.exit(1)
   elif sys.argv[1] == 'menu':
     ExtractMenu(args=sys.argv[2:]).run()
   elif sys.argv[1] == 'tips':
     ExtractTips(args=sys.argv[2:]).run()
   elif sys.argv[1] == 'reviews':
     ExtractReviews(args=sys.argv[2:]).run()
   elif sys.argv[1] == 'fs-mentions':
     AccumulateFoursquareMentions(args=sys.argv[2:]).run()
   elif sys.argv[1] == 'yelp-mentions':
     AccumulateYelpMentions(args=sys.argv[2:]).run()
   else:
     print >> sys.stderr, "Unrecognized job (expecting [menu, tips, reviews, fs-mentions, yelp-mentions])"
     sys.exit(1)
