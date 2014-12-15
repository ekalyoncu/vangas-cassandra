# Vangas Twitter Example

Spray.io based example to show the usage of vangas.
Data model is borrowed from [twissandra](https://github.com/twissandra/twissandra).

## How to run
* Execute statements in tables.cql in order to create tables.
* Run spray app by typing ```sbt run```

Here are the list of endpoints

* **POST** /v1/users to create users
* **POST** /v1/tweets to create tweet
* **GET** /v1/tweets to get list of tweets
* **GET** /v1/tweets/:tweet_id to get single tweet