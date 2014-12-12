import json
import csv
import nltk
from nltk import FreqDist
import os
from nltk.corpus import stopwords
import sqlite3
from pprint import pprint
import itertools

import openpyxl as px
import numpy as np

from sklearn import ensemble
from sklearn.preprocessing import LabelEncoder  

f = open("train.csv", "rU")
reader = csv.reader(f, delimiter=',', quotechar='"')
trainingset = []
ytrain = []
rownum = 0
for row in reader:
    if rownum == 0:
        header = row
    else:
        colnum = 0
        for col in row:
            if colnum == 4:
                v = col.translate(None, "[]'")
                categorylist = []
                categorylist = v.split()
                category_a = categorylist[0]
                category_b = categorylist[1]
            if colnum == 5:
                review_count = col
            if colnum == 6:
                stars = col
            if colnum == 10:
                no_tips = col
            if colnum == 13:
                group = col
                trainingset.append((category_a, category_b, review_count, stars, no_tips))
                ytrain.append(group)
            
            colnum +=1
            
            
                  
    rownum = rownum + 1
 
X_train = np.asarray(trainingset)
y = np.asarray(ytrain)

X_train[:, 0] = LabelEncoder().fit_transform(X_train[:,0]) 
X_train[:, 1] = LabelEncoder().fit_transform(X_train[:,1]) 

clf = ensemble.RandomForestClassifier(n_estimators = 500, n_jobs = -1)


clf.fit(X_train, y)


g = open("final.csv", "rU")
reader_test = csv.reader(g, delimiter=',', quotechar='"')

testingset = []
ytest = []
rownum = 0
for row in reader_test:
    if rownum == 0:
        header = row
    else:
        colnum = 0
        for col in row:
            if colnum == 1:
                test = col
            if colnum == 4:
                v = col.translate(None, "[]'")
                categorylist = []
                categorylist = v.split()
                category_a = categorylist[0]
                category_b = categorylist[1]
                
            if colnum == 5:
                review_count = col
            if colnum == 6:
                stars = col
            if colnum == 10:
                no_tips = col
                testingset.append((category_a, category_b, review_count, stars, no_tips))
            colnum +=1
            
            
                  
    rownum = rownum + 1

X_test = np.asarray(testingset)
X_test[:, 0] = LabelEncoder().fit_transform(X_test[:,0]) 
X_test[:, 1] = LabelEncoder().fit_transform(X_test[:,1]) 

final_predict = []
final_predict = clf.predict(X_test)
print final_predict

myfile = open("predict.csv", 'wb')
wr = csv.writer(myfile)
wr.writerow(final_predict)
