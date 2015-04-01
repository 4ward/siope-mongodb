# Siope Data Retriever
This script downloads data from [Siope] (https://www.siope.it) and populates a MongoDB.

Automatic steps:

1. Data retrieval
2. Aggregation of csv files _ENTRATE*.csv_ and _USCITE*.csv_ into *ENTRATE.CSV* and *USCITE.CSV*
3. Three steps in MongoDB:
  1. Each row of each csv file is insert as document in the collection corresponding (example of collection: *csv_entrate*)
  2. Build *mdb_\** collections. These are mongo style collections, in particular *mdb_entrate* and *mdb_uscite* where each income and outcome has more information about it (ente, for example)
  3. Build two "time series collections": *mdb_entrate_mensili* and *mdb_uscite_mensili* where you can find income and outcome grouped by year/period/cod_ente in an array of subdocuments.
