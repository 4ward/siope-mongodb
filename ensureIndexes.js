
//In this file you can ensure new indexes in order to speed up the aggregations

//In MongoDB 3 the new command to create index is db.collection.createIndex()
//We use db.collection.ensureIndex() that in MongoDB 3 is an alias to db.collection.createIndex()
//in order to maintain compatibility with mongo 2

//Use of this file

//In the shell run:

//mongo < ensureIndexes.js

//uncomment the following lines

//use siope2
//db.mdb_entrate_mensili.ensureIndex({ANNO:1})
//db.mdb_uscite_mensili.ensureIndex({ANNO:1})
//db.mdb_uscite.ensureIndex({ANNO:1,DESCR_ENTE:1})
//db.mdb_uscite_mensili.ensureIndex({ANNO:1,DESCR_ENTE:1})
//...