# Change log

## 0.2.1 ([#7](https://git.mobcastdev.com/Marvin/magrathea/pull/7) 2014-11-21 10:35:08)

Database migration scripts

### Improvements

* Added database migration scripts for PostgreSQL

## 0.2.0 ([#6](https://git.mobcastdev.com/Marvin/magrathea/pull/6) 2014-11-18 17:34:23)

Postgres

### Breaking Changes

* Switched from CouchDB to PostgreSQL 9.4beta3
* Greatly refactored and simplified `MessageHandler`
* Simplified `AppConfig`
* Updated Swagger doc

## 0.1.0 ([#5](https://git.mobcastdev.com/Marvin/magrathea/pull/5) 2014-10-31 16:39:27)

Search & Indexing

### New features

* Added search and index API
* Added document de-annotation
* Upgraded to Scala 2.11.4

## 0.0.4 ([#4](https://git.mobcastdev.com/Marvin/magrathea/pull/4) 2014-10-10 16:57:13)

Book retrieval implementation

### Improvement

* Added book retrieval API implementation

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

