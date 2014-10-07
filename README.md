# Yelpsquare

By: *Doug Kang, Gopala Tumuluri, Kevin Allen, Ying Chan*

Outlined below is our project proposal for DATASCI205: Storing and Retrieving Data.  In this proposal, we will attempt to exercise our knowledge in 2 of the 3 V's of Big Data: Variety and Volume.

Yelp and Foursquare are the two most popular reviews platform for restaurants and places.

Previously, users had scroll through pages and pages for reviews, and somewhat repetitive pictures of food (and sometimes pictures of people) to find what items in the menu are recommended at a restaurant, and for particular purpose (e.g. dating, casual outing).  Our web application will attempt to solve

## Data Question

The data question that we are trying to answer is twofold:
 - "What is the best [category] restaurant to eat in [region] with [type of group]?
 - "What is the best place thing to eat at a restaurant?

## Data Sources

We will pull from two main sources for our data:
- Foursquare API (https://developer.foursquare.com)
  - Restaurant Metadata, Ratings, Tips, Menu
- Yelp API (http://www.yelp.com/developers)
  - Restaurante Metadata, Ratings, Tips, Reviews

As time permits, we will also consider additional sources of restaurant rating and review data.

## Data Cleaning

In order to properly extract meaning from the data, we will try to clean the data:
- Lemmetization, stemming, and removal of stop words
- Combining of restaurant metadatas through fuzzy matching to join yelp/foursquare ids

## Data Analysis

- Use TF/IDF to identify key words within reviews 
- Normalize Yelp and Foursquare ratings to provide overall, aggregated rating
- Apply machine learning algorithms to learn restaurant's ideal target audience (i.e. group) based on reviews
- Apply sentiment and frequency analysis on reviews and tips to generate ranking of menu items 

## Architecture Overview

![](https://raw.githubusercontent.com/dougkang/ds205/master/docs/images/arch.png)

Our data flow will be comprised of several logical components:

- **Ingestion**
  - **Foursquare Tip Ingestion**: takes in Foursquare tip data and writes it in JSON format to S3.  This process is expected to run in a batch process every X minutes
  - **Foursquare Restaurant Data Extraction**: takes in Foursquare restaurant data (foursquareid, name, location, category, menu) and writes it in JSON format to S3.  This process is expected to run less frequently every X days
  - **Yelp Tip Ingestion**: takes in Yelp tip data and writes it in JSON format to S3.  This process is expected to run in a batch process every X minutes
  - **Yelp Review Ingestion**: takes in Yelp review data and writes it in JSON format to S3.  This process is expected to run in a batch process every X minutes
  - **Yelp Restaurant Data Extraction**: takes in Yelp restaurant data (yelpid, name, location, category) and writes it in JSON format to S3.  This process is expected to run less frequently every X days
- **ETL**
  - **Restaurant Fuzzy Matching**: a proper mapping between yelpids and foursquareids does not exist.  As such, we need to perform a fuzzy matching based on the name, address and possibly other metadata.
  - **Restaurant Menu Extraction**: responsible for pulling out menu data, if exists
  - **Rating Calculator**: Combines the foursquare and yelp rating to create a single score.  This will probably do some querying on the mongodb for review/tip ratings, etc.
  - **Restaurant Metadata Load**: Massages restaurant data into a format presentable to the user
  - **Review Data Load**: Massages review data into a format presentable to the user
  - **Review Data Cleaning**: Cleans review contents (stemming, lemmatization, removal of stop words, etc)
  - **Review/Tip Normalizer**: Somehow explodes a review into a number of tips (maybe every sentence?)
  - **Group Type Analysis**: Extracts the group type ("with my boyfriend", "with the guys", etc)
  - **Sentiment Analysis**: For each sentence, determines the sentiment of the sentence
  - **Menu Item Matching and Analysis**: For each sentence for a given restaurant and the menu of the restaurant, determine if a user is talking about an item on the menu
  - **Menu Item Data Load**: Score a menu item for a restaurant based on its frequency and sentiment
  - **Group Type Data Load**: Score a group type for a restaurant based on its frequency and sentiment

### Additional Notes
- We will leverage Spark to handle failover
- Intermediate data will be stored in S3 to facilitate debugging

## Technology Stack

While not set in stone, our technology stack will be as follows:

![](https://raw.githubusercontent.com/dougkang/ds205/master/docs/images/techstack.png)

- **Preferred Language of Choice**: Python (https://www.python.org)
  - Python was chosen mainly due to the readability of the language as well as the rich set of libraries.
  - Given that our members on average do not have a strong programming background, Python appeared to be the language that most of our members had at least some experience with and was easiest to pick up
  - That being said, we will not be limiting ourselves to Python and will use whatever language seems appropriate for each task.
- **Document Store**: MongoDB (http://www.mongodb.org)
  - We chose MongoDB mainly due to our data access needs. The heavy-read, less-frequent-write nature of our application makes MongoDB a strong choice
  - Also, given that our input data will be JSON and our output data will be in JSON, a BSON store seemed appropriate.
- **Unstructured Data Storage**: S3 (http://aws.amazon.com/s3/)
- **Distributed Execution Framework**: Spark (PySpark) (http://spark.apache.org)
  - Spark was chosen because of its strong tie with Python
- **Orchestration**: Chronos (http://airbnb.github.io/chronos/)
- **Web Server**: Flask (http://flask.pocoo.org)
  - Flask was chosen because of its lightweight nature and also because it is Python-based
- **Javascript Frameworks**: JQuery, Bootstrap

### Additional Notes
- Each component in the technology stack will be deployed onto EC2.

## Wireframe

The wireframe below is an attempt to show how the data will be presented:

![](https://raw.githubusercontent.com/dougkang/ds205/master/docs/images/wireframes.png)

1. User will select from the dropdown menu the [Category], [Region] and [Type of Group] and hit Search
2. A list of restaurants will be displayed in tabular form.  Results will be sorted by user relevance, based on the Category, Region, and Type of Group.
3. [Stretch Goal] A Google Map will also display the results.
4. User will click on one of the results to reveal a subsection that ranks menu items based on popularity.

## Responsibilities

Our group members all expressed the desire to experience all parts of the data pipeline process. As such, we broke up the task into a number of components.  Listed below are the tentative responsibilities.

- Doug
  - Foursquare Ingestion
  - Group Type Extraction and Analysis
- Gopala
  - Yelp Ingestion
  - Menu Item Matching & Analysis
  - Sentiment Analysis
- Kevin
  - Restaurant ETL Processing: Kevin
  - UI Layer
  - Sentiment Analysis
- Ying
  - Review and Tip Data Cleansing
  - Group Type Extraction and Analysis
