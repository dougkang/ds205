from flask import Flask
from flask import Markup
from flask import render_template
from flask import request
from flask.ext.pymongo import PyMongo

app = Flask(__name__)
with app.app_context():
  app.config.from_object('config')
  mongo = PyMongo(app, config_prefix='MONGO2')
  cities = mongo.db.restaurants.find({'$and':
    [{'hasfs':True}, {'hasyelp':True}, {'group': {'$exists':True}}]}
    ).distinct('city')
  cities.sort()
  categories = mongo.db.restaurants.find({'$and':
    [{'hasfs':True}, {'hasyelp':True}, {'group': {'$exists':True}}]}
    ).distinct('categories')
  categories.sort()
  groups = mongo.db.restaurants.find({'$and':
    [{'hasfs':True}, {'hasyelp':True}, {'group': {'$exists':True}}]}
    ).distinct('group')
  groups.sort()

@app.route('/')
def main_page():
  with app.app_context():
    return render_template('index.html', cities=cities, categories=categories,
      groups=groups)
    
@app.route('/search')
def search():
  city = request.args.get('city')
  group = request.args.get('group')
  food = request.args.get('food')
  with app.app_context():
    if food is not None and city is not None and group is not None:
      results = mongo.db.restaurants.find({'$and':[{'city':city},{'categories':
        {'$in': [food] }}, {'group':group}]}, {'_id':0,'name':1,'lat':1,
        'long':1,'city':1,'state':1,'addr':1,'rating':1,'postal_code':1,
        'categories':1,'yelpurl':1,'fsid':1}).sort('rating',-1).limit(50)
      results = [result for result in results]
      for result in results:
        try:
          menu_items = mongo.db.menu.find({'fsid':result['fsid']},{'_id':0,
            'name':1,'num_mentions':1}).sort('num_mentions',-1)
          menu = [menu_item for menu_item in menu_items]
          result['menu'] = menu
        except KeyError:
          result['menu'] = []
      return render_template('search.html', cities=cities, results=results,
        categories=categories, groups=groups, city=city, group=group, food=food)
    if food is not None and city is not None:
      results = mongo.db.restaurants.find({'$and':[{'city':city},{'categories':
        {'$in': [food] }}]}, {'_id':0,'name':1,'lat':1,'long':1,'city':1,
        'state':1,'addr':1,'rating':1,'postal_code':1,'categories':1,
        'yelpurl':1,'fsid':1}).sort('rating',-1).limit(50)
      results = [result for result in results]
      for result in results:
        try:
          menu_items = mongo.db.menu.find({'fsid':result['fsid']},{'_id':0,
            'name':1,'num_mentions':1}).sort('num_mentions',-1)
          menu = [menu_item for menu_item in menu_items]
          result['menu'] = menu
        except KeyError:
          result['menu'] = []
      return render_template('search.html', cities=cities, results=results,
        categories=categories, groups=groups, city=city, food=food)
    if group is not None and city is not None:
      results = mongo.db.restaurants.find({'$and':[{'city':city},
        {'group':group}]}, {'_id':0,'name':1,'lat':1,'long':1,'city':1,
        'state':1,'addr':1,'rating':1,'postal_code':1,'categories':1,
        'yelpurl':1,'fsid':1}).sort('rating',-1).limit(50)
      results = [result for result in results]
      for result in results:
        try:
          menu_items = mongo.db.menu.find({'fsid':result['fsid']},{'_id':0,
            'name':1,'num_mentions':1}).sort('num_mentions',-1)
          menu = [menu_item for menu_item in menu_items]
          result['menu'] = menu
        except KeyError:
          result['menu'] = []
      return render_template('search.html', cities=cities, results=results,
        categories=categories, groups=groups, city=city, group=group)
    if city is not None:
      results = mongo.db.restaurants.find({'city':city}, {'_id':0,'name':1,
      'lat':1,'long':1,'city':1,'state':1,'addr':1,'rating':1,'postal_code':1,
      'categories':1,'yelpurl':1,'fsid':1}).sort('rating',-1).limit(50)
      results = [result for result in results]
      for result in results:
        try:
          menu_items = mongo.db.menu.find({'fsid':result['fsid']},{'_id':0,
            'name':1,'num_mentions':1}).sort('num_mentions',-1)
          print menu_items.count()
          menu = [menu_item for menu_item in menu_items]
          result['menu'] = menu
          print menu
        except KeyError:
          result['menu'] = []
      return render_template('search.html', cities=cities, results=results,
        categories=categories, groups=groups, city=city)
    
if __name__=='__main__':
  #app.run(host='0.0.0.0', port=10080)
  app.run(debug=True)