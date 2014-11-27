from flask import Flask
from flask import Markup
from flask import render_template
from flask import request
from flask.ext.pymongo import PyMongo

app = Flask(__name__)
with app.app_context():
    app.config.from_object('config')
    mongo = PyMongo(app, config_prefix='MONGO')
    cities = mongo.db.restaurants.distinct('city')
    cities.sort()


@app.route('/')
def main_page():
  with app.app_context():
    return render_template('index.html', cities=cities)
    
@app.route('/search')
def show_results():
  city = request.args.get('city')
  return 'Results for %s' % city
    
if __name__=='__main__':
  app.run(debug=True)