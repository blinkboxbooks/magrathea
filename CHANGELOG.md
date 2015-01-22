# Change log

## 0.5.10 ([#21](https://git.mobcastdev.com/Marvin/magrathea/pull/21) 2015-01-22 11:57:40)

Recover from missing index

### Bug fixes

- Fixed an issue when searching and re-indexing when the index is absent

### Improvements

- Switched from cached thread pool to fork join thread pool

## 0.5.9 ([#20](https://git.mobcastdev.com/Marvin/magrathea/pull/20) 2015-01-14 17:43:32)

Sequence Number

### Improvements

- Added `sequenceNumber` in the distribution message, which is essentially a timestamp

## 0.5.8 ([#19](https://git.mobcastdev.com/Marvin/magrathea/pull/19) 2015-01-13 14:16:22)

Using rejection handler to return json errors

### Improvements

- Added rejections handler to jsonify all the common exceptions
- Switched to v2 Error for consistency

## 0.5.7 ([#18](https://git.mobcastdev.com/Marvin/magrathea/pull/18) 2015-01-13 11:23:38)

CORS Header

### Improvement

- Added `Access-Control-Allow-Origin: *` header for every API response

## 0.5.6 ([#17](https://git.mobcastdev.com/Marvin/magrathea/pull/17) 2015-01-12 14:46:52)

Correct the threads health check test

Patch

Add the health endpoint for testing devint, and correct thread -> threads

## 0.5.5 ([#16](https://git.mobcastdev.com/Marvin/magrathea/pull/16) 2015-01-12 14:30:49)

Re-indexing & Tests

### Improvements

- Removing previous indexes when re-indexing
- Fixed tests

## 0.5.4 ([#15](https://git.mobcastdev.com/Marvin/magrathea/pull/15) 2015-01-12 13:16:51)

ElasticHttp & Health Checks

### Improvements

- Switched to elastic-http
- Including the `id` of each search result
- Bumped libraries

### Bug fixes

- Fixed health endpoints


## 0.5.3 ([#14](https://git.mobcastdev.com/Marvin/magrathea/pull/14) 2015-01-07 15:26:58)

New name for magrathea

### Improvement

- Renamed magrathea to ingestion-metadata-service

## 0.5.2 ([#13](https://git.mobcastdev.com/Marvin/magrathea/pull/13) 2014-12-16 16:36:15)

Health endpoint tests

Patch

 * Cukes for testing the health endpoints are exposed as intended

## 0.5.1 ([#11](https://git.mobcastdev.com/Marvin/magrathea/pull/11) 2014-12-16 16:27:01)

Api automation

### Improvement

* This adds the most basic level of API functional tests

## 0.5.0 ([#10](https://git.mobcastdev.com/Marvin/magrathea/pull/10) 2014-12-05 13:58:18)

Deliver distributability

### New Feature

- Sending distribution information message

## 0.4.0 ([#9](https://git.mobcastdev.com/Marvin/magrathea/pull/9) 2014-12-04 16:52:35)

Distributability

### New feature

* Calculates the distributability of a given document according to the [business rules].(http://jira.blinkbox.local/confluence/pages/viewpage.action?title=Distribution+Business+Logic&spaceKey=QUILL)

## 0.3.0 ([#8](https://git.mobcastdev.com/Marvin/magrathea/pull/8) 2014-11-28 11:29:30)

Document History

### New features

- Document history implementation for the current documents 

### Improvements

- Bumped some versions
- Using the new scala-logging
- Improved and refactored tests
- Multi-project layout

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

