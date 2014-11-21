from flask import Flask
from flask import Markup
from flask import render_template

app = Flask(__name__)

@app.route('/')
def main_page():
    return render_template('index.html')
    
@app.route('/search/<city>')
def show_city(city):
    return 'Results for %s' % city
    
if __name__=='__main__':
    app.run(debug=True)