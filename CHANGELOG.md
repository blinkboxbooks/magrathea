# Change log

## 0.0.1 ([#1](https://git.mobcastdev.com/Marvin/magrathea/pull/1) 2014-09-12 17:02:56)

First cut of magrathea

### Improvements

* Listening for two specific type of messages from RabbitMQ
* Decides whether to replace a message on CouchDB or just store it
* Fetches updates from `history` db, merging them and storing the result on `latest` db

### What's missing

* Adding distribution information
* Sending a distribute or un-distribute message

