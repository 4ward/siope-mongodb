# Siope Data Retriever

##What is this
This is a Python Script that downloads data from [Siope] (https://www.siope.it) and populates **siope** db in MongoDB.

**Siope** (Sistema Informativo delle OPerazioni degli Enti pubblici) is a system of electronic detection of collections and payments made by the cashiers of italian public services.

##How it works

There are three automatic steps:

1. Data retrieval (download and unzip files)
2. Aggregation of csv files _ENTRATE*.csv_ and _USCITE*.csv_ (where * is the year) into *ENTRATE.CSV* and *USCITE.CSV*
3. Three steps in MongoDB:
 	1. Each row of each csv file is insert as document in the collection corresponding (examples of collection: *csv_entrate*, *csv_enti* and so on)
  	2. Creating _mdb\__\*<name_collection>. These are mongo style collections, in particular *mdb_entrate* and *mdb_uscite* where each income and outcome has more information about it (ente, for example). The other important collection is *mdb_enti*.
  	3. Creating two "time series collections": *mdb_entrate_mensili* and *mdb_uscite_mensili* where you can find income and outcome grouped by year/period/cod_ente in an array of subdocuments.
  	
You can run this script every Friday to update data. 

NB: 3.1 and 3.3 drop collections and recreate them. 3.2 is an incremental update.
  
##Instructions

###Script use

    usage: main.py [-h] [--download=True] [--download=False] [--host HOST]
               [--port PORT]

    Store Siope.it data in MongoDB

    optional arguments:
      -h, --help        show this help message and exit
      --download=True   (DEFAULT) Download data from Siope.it
      --download=False  Not download data from Siope.it, you must just have them
                        in csvfiles directory
      --host HOST       (DEFAULT: localhost) Hostname or IP address where mongod
                        is running
      --port PORT       (DEFAULT: 27017) Port used by mongod process

The script may take several minutes.
** You can use pypy to speed up the process! ** (~50% faster)

###Queries

*queries.js* contains some sample queries. To use this:

    > mongo
    > use siope
    > load('queries.js')
    > queries.<function_name>.([year])
    
 Example of [Aggregation Framework] (http://docs.mongodb.org/manual/core/aggregation-pipeline/):
 	
 	> queries.entratePerEnte(2015)
	function (){
			return db.mdb_entrate_mensili.aggregate([
				{$match : {'ANNO' : anno}},
				{$unwind : '$IMPORTI'},
				{$group : {_id : {'ENTE' : '$DESCR_ENTE'}, 'Totale': {$sum : '$IMPORTI.IMPORTO'}}},
				{$sort : {'Totale' : -1}},
				{$project : {'Totale' : { $divide : ['$Totale', 100000000000]}}},
				{$project : {'Totale Miliardi €' : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
				{$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}}
				]); 
		}
	Execution Time: **** ms
	<List of json docs>
	
If you want to store some aggregated result in a collection you can use Map-Reduce.

Example of [Map-Reduce] (http://docs.mongodb.org/manual/core/map-reduce/):

	> queries.uscitePerEnteMR(2015)
	function (){
                        return db.mdb_uscite_mensili.mapReduce(
                                function() {
                                        lista = [];
                                        this.IMPORTI.forEach(function(x){
                                                lista.push(x['IMPORTO']/100000000000);
                                        });
                                        emit(this.DESCR_ENTE, Math.round(Array.sum(lista)*100)/100);
                                },
                                function(key, values){return Array.sum(values)},
                                {
                                        query: { ANNO: anno },
                                        out: "uscitePerEnteMR"
                                }
                        )
                }
And then:

	> db.uscitePerEnteMR.find()

or

	> db.uscitePerEnteMR.find().sort({value:-1})

...or whatever you want to do. 

###Indexes
You can use *ensureIndexes.js* file to ensure new indexes on collections.
Open and modify it as you want.

To use this, run:

	mongo < ensureIndexes.js
    
##Dependencies and compatibility

You may need to install the module **pymongo**.

Link: <https://pypi.python.org/pypi/pymongo/>

You can install it with pip <https://pypi.python.org/pypi/pip>

Test cases:

- Python 2 (Mac OS X, Ubuntu)
- Python 3 (Mac OS X, Ubuntu, Windows 10)

##License

	The MIT License (MIT)
	
	Copyright (c) 2015 Massimiliano Scotti
	
	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:
	
	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.
	
	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.

    
    

    
