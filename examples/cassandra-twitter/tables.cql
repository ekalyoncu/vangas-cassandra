CREATE KEYSPACE vangas_twitter WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};

use vangas_twitter;

CREATE TABLE users (
    username text PRIMARY KEY,
    password text
);

CREATE TABLE friends (
    username text,
    friend text,
    since timestamp,
    PRIMARY KEY (username, friend)
);

CREATE TABLE followers (
    username text,
    follower text,
    since timestamp,
    PRIMARY KEY (username, follower)
);

CREATE TABLE tweets (
    tweet_id timeuuid PRIMARY KEY,
    username text,
    body text
);