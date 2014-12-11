from flask import Flask
from flask import Markup
from flask import render_template
from flask import request
from flask.ext.pymongo import PyMongo

app = Flask(__name__)
with app.app_context():
    app.config.from_object('config')
    mongo = PyMongo(app, config_prefix='MONGO2')
    cities = mongo.db.restaurants.distinct('city')
    cities.sort()


@app.route('/')
def main_page():
  with app.app_context():
    return render_template('index.html', cities=cities)
    
@app.route('/search')
def show_results():
  city = request.args.get('city')
  group = request.args.get('group')
  food = request.args.get('food')
  with app.app_context():
    results = mongo.db.restaurants.find({'city':city}, {'name':1,'lat':1,
      'long':1,'city':1,'state':1,'addr':1}).limit(10)
    results = [result for result in results]
    return render_template('search.html', cities=cities, city=city, group=group, food=food, results=results)
    
if __name__=='__main__':
  app.run(host='0.0.0.0', port=10080)