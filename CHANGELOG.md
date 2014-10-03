# Change log

## 0.0.3 ([#3](https://git.mobcastdev.com/Marvin/magrathea/pull/3) 2014-10-03 15:32:07)

Additional contributor merge

### Improvements

* Whenever a book documents includes information about contributor(s), we initiate a contributor merge for each one of them
* Added test cases for history document normalisation, along with contributor merge
* Minor refactoring

## 0.0.2 ([#2](https://git.mobcastdev.com/Marvin/magrathea/pull/2) 2014-09-29 09:36:43)

New algorithm

### Improvements

* Each document gets annotated before merging
* Re-written the document merging algorithm

Annotating the document means that every non-object and non-array field has a value and a source. For Objects and classified-arrays, each child is converted to a field with a value and a source. Non-classified arrays are converted to fields with a value and a source.

## 0.0.1 ([#1](https://git.mobcastdev.com/Marvin/magrathea/pull/1) 2014-09-12 17:02:56)

First cut of magrathea

### Improvements

* Listening for two specific type of messages from RabbitMQ
* Decides whether to replace a message on CouchDB or just store it
* Fetches updates from `history` db, merging them and storing the result on `latest` db

### What's missing

* Adding distribution information
* Sending a distribute or un-distribute message
