import json
import csv
import random
import argparse
import configparser

import numpy as np

from sklearn import ensemble
from sklearn.preprocessing import LabelEncoder  
from sklearn import metrics 

def extract_features(row):
  categories = x[6].split("|")
  categories += ['NA'] * 3

  return [ int(x[0]), 
           x[1], 
           x[5], 
           x[7],
           x[8],
           x[9],
           categories[0],
           categories[1],
           categories[2] ]

def dummy_code(g):
  if g == "dating":
    return 0
  elif g == "men":
    return 1
  elif g == "women":
    return 2
  elif g == "family":
    return 3
  else:
    return 4

def dummy_uncode(i):
  if i == 0:
    return "Date Night" 
  elif i == 1:
    return "Guys Night Out"
  elif i == 2:
    return "Girls Night Out"
  elif i == 3:
    return "Family"
  else:
    return "Quick Bite"


if __name__ == '__main__':
  # Parse input arguments
  parser = argparse.ArgumentParser(description='Extracts Foursquare features from ingestion results')
  parser.add_argument('--config', metavar='c', required=False, type=str, 
      default='./config.ini', help='path to config file')
  parser.add_argument('--features', metavar='b', required=False, type=str, 
      default='./train.csv', help='path to train.csv')
  parser.add_argument('--dataset', metavar='b', required=False, type=str, 
      help='path to dataset to be evaluated')
  parser.add_argument('--output', metavar='o', required=False, type=str, 
      default='./fs_features.json', help='path to output file')
  args = parser.parse_args()

  with open(args.features, 'rU') as f:
    data = [ x for x in csv.reader(f, delimiter=',', quotechar='"') ]
    random.shuffle(data)
  
    features = [ extract_features(x) for x in data ]
    outcomes = [ dummy_code(x[-1]) for x in data ]
  
    print "Found %d entries" % len(data)

    if args.dataset is None: 
      print "Dataset to evaluate is not provided. Using training set"
      n_train = int(0.80 * len(features))
      train_features = features[0:n_train]
      train_outcomes = outcomes[0:n_train]
      test_features = features[n_train:]
      test_outcomes = outcomes[n_train:]
  
      print "Training against %d entries" % len(train_features)
    else:
      print "Dataset to evaluate provided"
      train_features = features
      train_outcomes = outcomes
      with open(args.dataset, 'rU') as f_data:
        data = [ x for x in csv.reader(f_data, delimiter=',', quotechar='"') ]
        test_ids = [ x[3] for x in data ]
        test_features = [ extract_features(x) for x in data ]
        test_outcomes = [ dummy_code(x[-1]) for x in data ]
  
    print "Training against %d entries" % len(train_features)
   
    X_train = np.asarray(train_features)
    Y_train = np.asarray(train_outcomes)
    X_train[:, 1] = LabelEncoder().fit_transform(X_train[:, 1])
    X_train[:, 2] = LabelEncoder().fit_transform(X_train[:, 2])
    X_train[:, 6] = LabelEncoder().fit_transform(X_train[:, 6])
    X_train[:, 7] = LabelEncoder().fit_transform(X_train[:, 7])
    X_train[:, 8] = LabelEncoder().fit_transform(X_train[:, 8])
    
    Y_train[:] = LabelEncoder().fit_transform(Y_train[:])
    
    clf = ensemble.RandomForestClassifier(n_estimators = 500, criterion = 'entropy', max_depth = 5)
    clf.fit(X_train, Y_train)
 
    X_test = np.asarray(test_features)
    Y_test = np.asarray(test_outcomes)
    X_test[:, 1] = LabelEncoder().fit_transform(X_test[:, 1])
    X_test[:, 2] = LabelEncoder().fit_transform(X_test[:, 2])
    X_test[:, 6] = LabelEncoder().fit_transform(X_test[:, 6])
    X_test[:, 7] = LabelEncoder().fit_transform(X_test[:, 7])
    X_test[:, 8] = LabelEncoder().fit_transform(X_test[:, 8])
   
    Y_pred = clf.predict(X_test)

    if args.dataset is not None: 
      print "Outputting results"
      with open(args.output, 'w') as f_output:
        for (x, y) in zip(test_ids, Y_pred):
          f_output.write("%s\t%s\n" % (x, dummy_uncode(y)))
    else:
      Y_test[:] = LabelEncoder().fit_transform(Y_test[:])
      print(metrics.classification_report(Y_pred, Y_test))
      
