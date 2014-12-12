import json
import csv
import nltk
import os
from nltk.corpus import stopwords
import sqlite3


def create_database(dbname):
    conn = sqlite3.connect(dbname)
    return conn
    
def create_table(cur,tname, var1, var2, var3):
    query = 'CREATE TABLE '+tname+' ('+var1+', '+var2+ ', ' +var3+')'
    cur.execute(query)
    return cur

def insert_data(cur,tname,var1, var2, var3):
    query = 'INSERT INTO '+tname+' VALUES ("'+var1+'","'+var2+'","'+var3+'")'
    cur.execute(query)
    return cur

def create_table2(cur,tname, var1, var2, var3, var4, var5, var6, var7):
    query = 'CREATE TABLE '+tname+' ('+var1+', '+var2+ ', ' +var3+',' +var4+','+var5+','+var6+','+var7+')'
    cur.execute(query)
    return cur

def insert_data2(cur,tname,var1, var2, var3, var4, var5, var6, var7):
    query = 'INSERT INTO '+tname+' VALUES ("'+var1+'","'+var2+'","'+var3+'","'+var4+'","'+var5+'","'+var6+'","'+var7+'")'
    cur.execute(query)
    return cur

def create_table3(cur,tname, var1, var2, var3, var4, var5, var6, var7, var8, var9, var10, var11, var12):
    query = 'CREATE TABLE '+tname+' ('+var1+', '+var2+ ', ' +var3+',' +var4+','+var5+','+var6+','+var7+','+var8+','+var9+','+var10+','+var11+','+var12+')'
    cur.execute(query)
    return cur

def insert_data3(cur,tname,var1, var2, var3, var4, var5, var6, var7, var8, var9, var10, var11, var12):
    query = 'INSERT INTO '+tname+' VALUES ("'+var1+'","'+var2+'","'+var3+'","'+var4+'","'+var5+'","'+var6+'","'+var7+'","'+var8+'","'+var9+'","'+var10+'","'+var11+'","'+var12+'")'
    cur.execute(query)
    return cur

def run_query(cur):
    query = 'select a.business_id, a.name, address, a.city, a.categories, review_count, a.stars, review_text, tips, price_tier, no_tips, price, c.categories from reviews a left join businesses b on a.business_id = b.business_id  left join foursquare c on a.business_id = c.yelp_id where a.categories like \'%restaurants%\' '
    cur.execute(query)
    final = csv.writer(open("final.csv", "wb+"))
    final.writerow(["business_id","name", "address", "city", "categories", "review_count", "stars", "review_text", "tips", "price_tier", "no_tips", "price", "foursquare_categories"])
    
    for row in cur:
        if row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and row[6] is not None and row[7] is not None and row[8] is not None and row[9] is not None and row[10] is not None and row[11] is not None and row[12] is not None : 
        
            final.writerow([row[0].encode('utf-8'), row[1].encode('utf-8'), row[2].encode('utf-8'), row[3].encode('utf-8'), row[4].encode('utf-8'), row[5].encode('utf-8'), row[6].encode('utf-8'), row[7].encode('utf-8'), row[8].encode('utf-8'), row[9].encode('utf-8'), row[10].encode('utf-8'), row[11].encode('utf-8'), row[12].encode('utf-8')])
    return cur

def _decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        rv.append(item)
    return rv

def _decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)
        rv[key] = value
    return rv

#load SQL
dbname = 'yelp.db'   
table = 'businesses'
table2 = 'reviews'
table3 = 'foursquare'
conn = create_database(dbname)
cur = conn.cursor()

cur = create_table(cur,table,'business_id','stars', 'review_text')
            
#load review file
rev = csv.writer(open("review.csv", "wb+"))
rev.writerow(["business_id","stars", "review_text"])

for line in open('yelp_reviews.json'):
  line = line.strip()
  #print json.dumps(line, sort_keys=True, indent = 4)
  x = json.loads(line, object_hook=_decode_dict)
  #print x["user_id"]
  rev.writerow([x["business_id"], x["stars"], x["text"]])
  cur = insert_data(cur,table,str(x["business_id"]).replace("\"","'"), str(x["stars"]).replace("\"","'"), str(x["text"]).replace("\"","'"))

cur = create_table2(cur,table2,'business_id','name','address','city','categories','review_count','stars')  
      
bus = csv.writer(open("businesses.csv", "wb+"))
bus.writerow(["business_id","name", "address", "city", "categories", "review_count", "stars"])

for line in open('yelp_businesses.json'):
    line = line.strip()
    y = json.loads(line, object_hook=_decode_dict)
    bus.writerow([y["business_id"], y["name"], y["full_address"], y["city"], y["categories"], y["review_count"], y["stars"]])
    cur = insert_data2(cur,table2,str(y["business_id"]).replace("\"","'"), str(y["name"]).replace("\"","'"), str(y["full_address"]).replace("\"","'"), str(y["city"]).replace("\"","'"), str(y["categories"]).replace("\"","'"), str(y["review_count"]).replace("\"","'"),str(y["stars"]).replace("\"","'") )


cur = create_table3(cur,table3,'categories', 'id', 'house_no', 'tips', 'price_tier', 'city', 'name', 'no_tips', 'price', 'yelp_id', 'state', 'street')

foursq = csv.writer(open("foursquare.csv", "wb+"))
foursq.writerow(["categories", "id", "house_no", "tips", "price_tier", "city", "name", "no_tips", "price", "yelp_id", "state", "street"])

for line in open('foursquare_academic_features.json'):
    line = line.strip()
    y = json.loads(line, object_hook=_decode_dict)
    if y.get('tips'):
        foursq.writerow([y["categories"], y["id"], y["house_no"],  y["tips"],  y["price_tier"], y["city"], y["name"], y["no_tips"], y["price"], str(y["yelpid"]).replace("\n"," "), y["state"], y["street"]])
        cur = insert_data3(cur,table3,str(y["categories"]).replace("\"","'"),str(y["id"]).replace("\"","'"),str(y["house_no"]).replace("\"","'"),str(y["tips"]).replace("\"","'"),str(y["price_tier"]).replace("\"","'"),str(y["city"]).replace("\"","'"),str(y["name"]).replace("\"","'"),str(y["no_tips"]).replace("\"","'"),str(y["price"]).replace("\"","'"),str(y["yelpid"]).replace("\"","'"),str(y["state"]).replace("\"","'"),str(y["street"]).replace("\"","'")   )
    else: 
        foursq.writerow([y["categories"], y["id"], y["house_no"],  " ",  y["price_tier"], y["city"], y["name"], "0", y["price"], str(y["yelpid"]).replace("\n"," "), y["state"], y["street"]])
        cur = insert_data3(cur,table3,str(y["categories"]).replace("\"","'"),str(y["id"]).replace("\"","'"),str(y["house_no"]).replace("\"","'"),str(" ").replace("\"","'"),str(y["price_tier"]).replace("\"","'"),str(y["city"]).replace("\"","'"),str(y["name"]).replace("\"","'"),str("0").replace("\"","'"),str(y["price"]).replace("\"","'"),str(y["yelpid"]).replace("\"","'"),str(y["state"]).replace("\"","'"),str(y["street"]).replace("\"","'")   )
    

cur = run_query(cur)     
           
conn.commit()
conn.close()

