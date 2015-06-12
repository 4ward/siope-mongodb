
//In this file you can ensure new indexes in order to speed up the aggregations

//In MongoDB 3 the new command to create index is db.collection.createIndex()
//We use db.collection.ensureIndex() that in MongoDB 3 is an alias to db.collection.createIndex()
//in order to maintain compatibility with mongo 2

//For example, a lot of aggregations have $match on ANNO field in pipeline
//so we ensure a new index on the collections mdb_entrate_mensili and mdb_uscite_mensili

//Use of this file

//In the shell run:

//mongo < ensureIndexes.js

use siope2
db.mdb_entrate_mensili.ensureIndex({ANNO:1})
db.mdb_uscite_mensili.ensureIndex({ANNO:1})