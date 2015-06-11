#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Massimiliano Scotti"
__license__ = "MIT License"

import pymongo
import csv
import sys
import multiprocessing as mp
import glob
import subprocess as sp
import os
from shutil import copyfileobj
import zipfile
import argparse
#try Python 2 import
try:
    from urllib import urlretrieve
#else import Python 3 module
except ImportError:
    from urllib.request import urlretrieve

#FUNCTIONS

#(STEP 1)
#retrieve data: download, unzip of files from siope website
#entrate_aggregation()
#uscite_aggregation()
#csv_*(): map csv rows to mongo documents
#table_to_collection(): calls function above

#(STEP 2)
#build_collection_mdb(): creates collections mdb_*, in particular mdb_entrate/uscite
#where each document is an income or outcome with ente's informations
#creating_entrate_mdb() and creating_entrate_mdb() (and their helpers) 
#are the most important functions called by build_collection_mdb

#build_timeseries(): creates collections entrate/uscite grouping by ente
#every ente has an array of income or outcome

#This is a global variable that defines socket where mongod process is waiting for new connections
#Don't change it if you want to use mongodb://localhost:27017
socket = None

def retrieve_data():

    for f in os.listdir('.'):
        os.remove(f)

    zips = ['SIOPE_ANAGRAFICHE.zip',
            'SIOPE_USCITE.2015.zip','SIOPE_ENTRATE.2015.zip',
            'SIOPE_USCITE.2014.zip','SIOPE_ENTRATE.2014.zip',
            'SIOPE_USCITE.2013.zip','SIOPE_ENTRATE.2013.zip']

    for z in zips:
        print('Downloading file %s' %z)
        url = 'https://www.siope.it/Siope2Web/documenti/siope2/open/last/' + z
        todownload = True
        ndownload = 0
        while todownload == True and ndownload < 10:
            try:
                ndownload += 1
                urlretrieve(url, filename = z)
            except:
                if not ndownload < 10:
                    print('Warning: file %s not downloaded' %z)
            else:
                todownload = False
        try:
            zfile = zipfile.ZipFile(z)
            zfile.extractall()
            os.remove(z)
        except:
            pass


def entrate_aggregation():

    entrate = glob.glob('ENTRATE*')

    print('AGGREGATION OF INPUT...')
    
    with open('ENTRATE.csv', 'w') as output_file:
        for filename in entrate:
            with open(filename, 'r') as file:
                copyfileobj(file, output_file, -1)


def uscite_aggregation():

    uscite = glob.glob('USCITE*')

    print('AGGREGATION OF OUTPUT...')

    with open('USCITE.csv', 'w') as output_file:
        for filename in uscite:
            with open(filename, 'r') as file:
                copyfileobj(file, output_file, -1)


def csv_enti(path):

    print('CREATING csv_enti')

    connection = pymongo.MongoClient(socket)
    db = connection.siope2

    db.csv_enti.drop()
    bulk = db.csv_enti.initialize_unordered_bulk_op()

    fieldnames = ['COD_ENTE','DATA_INC_SIOPE','DATA_ESC_SIOPE',
                    'COD_FISCALE','DESCR_ENTE','COD_COMUNE','COD_PROVINCIA',
                        'NUM_ABITANTI','SOTTOCOMPARTO_SIOPE']

    with open(path) as csvfile:
        reader = csv.DictReader(csvfile,fieldnames=fieldnames)
        for row in reader:
            bulk.insert(row)
        bulk.execute()


def csv_comparti(path):

    print('CREATING csv_comparti')
    
    connection = pymongo.MongoClient(socket)
    db = connection.siope2

    db.csv_comparti.drop()
    bulk = db.csv_comparti.initialize_unordered_bulk_op()

    fieldnames = ['COD_COMPARTO','DESCRIZIONE_COMPARTO']

    with open(path) as csvfile:
        reader = csv.DictReader(csvfile,fieldnames=fieldnames)
        for row in reader:
            bulk.insert(row)
        bulk.execute()


def csv_sottocomparti(path):

    print('CREATING csv_sottocomparti')
    
    connection = pymongo.MongoClient(socket)
    db = connection.siope2

    db.csv_sottocomparti.drop()
    bulk = db.csv_sottocomparti.initialize_unordered_bulk_op()

    fieldnames = ['SOTTOCOMPARTO','DESCRIZIONE','COD_COMPARTO']

    with open(path) as csvfile:
        reader = csv.DictReader(csvfile,fieldnames=fieldnames)
        for row in reader:
            bulk.insert(row)
        bulk.execute()


def csv_comuni(path):

    print('CREATING csv_comuni')

    connection = pymongo.MongoClient(socket)
    db = connection.siope2
    db.csv_comuni.drop()
    bulk = db.csv_comuni.initialize_unordered_bulk_op()

    fieldnames = ['COD_COMUNE','DESCR_COMUNE','COD_PROVINCIA']

    with open(path) as csvfile:
        reader = csv.DictReader(csvfile,fieldnames=fieldnames)
        for row in reader:
            bulk.insert(row)
        bulk.execute()


def csv_regprov(path):

    print('CREATING csv_regprov')

    connection = pymongo.MongoClient(socket)
    db = connection.siope2
    db.csv_regprov.drop()
    bulk = db.csv_regprov.initialize_unordered_bulk_op()

    fieldnames = ['RIPART_GEO','COD_REGIONE','DESCRIZIONE REGIONE',
                    'COD_PROVINCIA','DESCRIZIONE_PROVINCIA']

    with open(path) as csvfile:
        reader = csv.DictReader(csvfile,fieldnames=fieldnames)
        for row in reader:
            bulk.insert(row)
        bulk.execute()


def csv_codgest_entrate(path):

    print('CREATING csv_codgest_entrate')

    connection = pymongo.MongoClient(socket)
    db = connection.siope2
    db.csv_codgest_entrate.drop()
    bulk = db.csv_codgest_entrate.initialize_unordered_bulk_op()

    fieldnames = ['COD_GEST','COD_CATEG','DESCRIZIONE_CGE',
                    'DATA_INIZIO_VALIDITA','DATA_FINE_VALIDITA']

    with open(path) as csvfile:
        reader = csv.DictReader(csvfile,fieldnames=fieldnames)
        for row in reader:
            bulk.insert(row)
        bulk.execute()


def csv_codgest_uscite(path):

    print('CREATING csv_codgest_uscite')

    connection = pymongo.MongoClient(socket)
    db = connection.siope2

    db.csv_codgest_uscite.drop()
    bulk = db.csv_codgest_uscite.initialize_unordered_bulk_op()

    fieldnames = ['COD_GEST','COD_CATEG','DESCRIZIONE_CGU',
                    'DATA_INIZIO_VALIDITA','DATA_FINE_VALIDITA']

    with open(path) as csvfile:
        reader = csv.DictReader(csvfile,fieldnames=fieldnames)
        for row in reader:
            bulk.insert(row)
        bulk.execute()


def csv_entrate(path):

    print('CREATING csv_entrate')

    connection = pymongo.MongoClient(socket)
    db = connection.siope2

    db.csv_entrate.drop()
    bulk = db.csv_entrate.initialize_unordered_bulk_op()

    fieldnames = ['COD_ENTE','ANNO','PERIODO',
                    'CODICE_GESTIONALE','IMP_USCITE_ATT']

    with open(path) as csvfile:
        reader = csv.DictReader(csvfile,fieldnames=fieldnames)
        i=0
        for row in reader:
            bulk.insert(row)
            i+=1
            if i%10000==0:
                bulk.execute()
                bulk = db.csv_entrate.initialize_unordered_bulk_op()
        try:        
            bulk.execute()
        except pymongo.errors.InvalidOperation:
            pass


def csv_uscite(path):

    print('CREATING csv_uscite')

    connection = pymongo.MongoClient(socket)
    db = connection.siope2
    db.csv_uscite.drop()
    bulk = db.csv_uscite.initialize_unordered_bulk_op()

    fieldnames = ['COD_ENTE','ANNO','PERIODO',
                    'CODICE_GESTIONALE','IMP_USCITE_ATT']

    with open(path) as csvfile:
        reader = csv.DictReader(csvfile,fieldnames=fieldnames)
        i=0
        for row in reader:
            bulk.insert(row)
            i+=1
            if i%10000==0:
                bulk.execute()
                bulk = db.csv_uscite.initialize_unordered_bulk_op()
        try:        
            bulk.execute()
        except pymongo.errors.InvalidOperation:
            pass


def table_to_collection(download=True):

    if not os.path.exists('./csvfiles'):
        os.makedirs('./csvfiles')
    os.chdir('csvfiles')

    #Scarico i dati aggiornati 
    print('DATA RETRIEVAL AND AGGREGATION OF INPUTS AND OUTPUTS')
    if download:
        retrieve_data()

    #Aggrego ENTRATE ed USCITE degli ultimi anni
    entrate_agg = mp.Process(target=entrate_aggregation)
    uscite_agg = mp.Process(target=uscite_aggregation)
    entrate_agg.start()
    uscite_agg.start()
    entrate_agg.join()
    uscite_agg.join()
    
    print('*** CREATING CSV COLLECTIONS *** [Step 1/3]')
    #Scrivo in ogni collezione del db siope2 con un processo per collezione
    p1 = mp.Process(target=csv_enti,args=(glob.glob('*ENTI_SIOPE*.csv')[0],))
    p2 = mp.Process(target=csv_comparti,args=(glob.glob('*_COMPARTI*.csv')[0],))
    p3 = mp.Process(target=csv_sottocomparti,args=(glob.glob('*SOTTOCOMPARTI*.csv')[0],))
    p4 = mp.Process(target=csv_comuni,args=(glob.glob('*COMUNI*.csv')[0],))
    p5 = mp.Process(target=csv_regprov,args=(glob.glob('*REG_PROV*.csv')[0],))
    p6 = mp.Process(target=csv_codgest_entrate,args=(glob.glob('*CODGEST_ENTRATE*.csv')[0],))
    p7 = mp.Process(target=csv_codgest_uscite,args=(glob.glob('*CODGEST_USCITE*.csv')[0],))
    #---------------------------------------------------------------------------------------
    #Invert the comments of the following lines to populate your database with all the years
    #---------------------------------------------------------------------------------------
    #p8 = mp.Process(target=csv_entrate,args=(glob.glob('ENTRATE.csv')[0],)).start()
    p8 = mp.Process(target=csv_entrate,args=(glob.glob('ENTRATE_2015*.csv')[0],))
    #p9 = mp.Process(target=csv_uscite,args=(glob.glob('USCITE.csv')[0],)).start()
    p9 = mp.Process(target=csv_uscite,args=(glob.glob('USCITE_2015*.csv')[0],))

    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    p7.start()
    p8.start()
    p9.start()
    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()
    p7.join()
    p8.join()
    p9.join()


def build_collection_mdb():

    print('*** CREATING MDB COLLECTIONS *** [Step 2/3]')
    connection = pymongo.MongoClient(socket)
    db = connection.siope2
    
    db.csv_sottocomparti.create_index([('SOTTOCOMPARTO',pymongo.ASCENDING)])
    db.csv_comparti.create_index([('COD_COMPARTO',pymongo.ASCENDING)])
    db.csv_regprov.create_index([('COD_PROVINCIA',pymongo.ASCENDING)])
    db.csv_comuni.create_index([('COD_COMUNE',pymongo.ASCENDING)])

    print('CREATING mdb_enti')

    enti = db.csv_enti.find()
    db.mdb_enti.create_index([('COD_ENTE',pymongo.ASCENDING)])
    bulk = db.mdb_enti.initialize_unordered_bulk_op()
    i = 0
    for ente in enti:

        if db.mdb_enti.find_one({'COD_ENTE':ente['COD_ENTE']}) is not None:
            continue
        
        ente_r = {  'COD_ENTE' : ente['COD_ENTE'],
                    'DATA_INC_SIOPE' : ente['DATA_INC_SIOPE'],
                    'DATA_ESC_SIOPE' : ente['DATA_ESC_SIOPE'],
                    'COD_FISCALE' : ente['COD_FISCALE'],
                    'DESCR_ENTE' : ente['DESCR_ENTE'],
                    'COD_COMUNE' : ente['COD_COMUNE'],
                    'COD_PROVINCIA' : ente['COD_PROVINCIA'],
                    'NUM_ABITANTI' : int(ente['NUM_ABITANTI'] or 0),
                    'COD_SOTTOCOMPARTO' : ente['SOTTOCOMPARTO_SIOPE']}
        
        sc = db.csv_sottocomparti.find_one({'SOTTOCOMPARTO' : ente_r['COD_SOTTOCOMPARTO']})
        
        ente_r.update({ 'DESCR_SOTTOCOMPARTO': sc['DESCRIZIONE'],
                        'COD_COMPARTO' : sc['COD_COMPARTO']})

        c = db.csv_comparti.find_one({'COD_COMPARTO' : ente_r['COD_COMPARTO']})

        ente_r.update({'DESCR_COMPARTO' : c['DESCRIZIONE_COMPARTO']})

        p = db.csv_regprov.find_one({'COD_PROVINCIA' : ente_r['COD_PROVINCIA']})

        ente_r.update({ 'DESCR_PROVINCIA' : p['DESCRIZIONE_PROVINCIA'],
                        'DESCR_REGIONE': p['DESCRIZIONE REGIONE'],
                        'COD_REGIONE' : p['COD_REGIONE'],
                        'RIPART_GEO' : p['RIPART_GEO']})

        c = db.csv_comuni.find_one({'COD_COMUNE' : ente_r['COD_COMUNE']})

        ente_r.update({'DESCR_COMUNE' : c['DESCR_COMUNE']})

        bulk.insert(ente_r)
        i += 1

        if i%10000==0:
            bulk.execute()
            bulk = db.mdb_enti.initialize_unordered_bulk_op()

    try:
        bulk.execute()
    except pymongo.errors.InvalidOperation:
        pass
    
    print('CREATING mdb_codgest_entrate')

    db.mdb_codgest_entrate.drop()
    db.mdb_codgest_entrate.create_index([('COD_GEST',pymongo.ASCENDING),('COD_CATEG',pymongo.ASCENDING)])
    csv_codgest_entrate = db.csv_codgest_entrate.find()
    bulk = db.mdb_codgest_entrate.initialize_unordered_bulk_op()

    for el in csv_codgest_entrate:
        el['DESCRIZIONE_CG'] = el.pop('DESCRIZIONE_CGE')
        bulk.insert(el)
    bulk.execute()

    print('CREATING mdb_codgest_uscite')

    db.mdb_codgest_uscite.drop()
    db.mdb_codgest_uscite.create_index([('COD_GEST',pymongo.ASCENDING),('COD_CATEG',pymongo.ASCENDING)])
    csv_codgest_uscite = db.csv_codgest_uscite.find()
    bulk = db.mdb_codgest_uscite.initialize_unordered_bulk_op()

    for el in csv_codgest_uscite:
        el['DESCRIZIONE_CG'] = el.pop('DESCRIZIONE_CGU')
        bulk.insert(el)
    bulk.execute()

    p1 = mp.Process(target=creating_entrate_mdb)
    p2 = mp.Process(target=creating_uscite_mdb)
    p1.start()
    p2.start()
    p1.join()
    p2.join()


def creating_entrate_mdb():

    print('CREATING mdb_entrate')

    connection = pymongo.MongoClient(socket)
    db = connection.siope2

    db.mdb_entrate.create_index([('COD_ENTE',pymongo.ASCENDING),('ANNO',pymongo.ASCENDING),
                                    ('PERIODO',pymongo.ASCENDING),('COD_GEST',pymongo.ASCENDING)])

    num_documents_csv_entrate = db.csv_entrate.count()
    index = num_documents_csv_entrate//2
    csv_entrate_head = db.csv_entrate.find().limit(index)
    csv_entrate_tail = db.csv_entrate.find().skip(index)

    entrateA = mp.Process(target=creating_entrate_mdb_helper, args=(csv_entrate_head,))
    entrateB = mp.Process(target=creating_entrate_mdb_helper, args=(csv_entrate_tail,))
    entrateA.start()
    entrateB.start()
    entrateA.join()
    entrateB.join()


def creating_entrate_mdb_helper(cursor):

    connection = pymongo.MongoClient(socket)
    db = connection.siope2

    bulk = db.mdb_entrate.initialize_unordered_bulk_op()

    i = 0
    for e in cursor:

        result = db.mdb_entrate.find_one({'COD_ENTE':e['COD_ENTE'],'ANNO':int(e['ANNO'] or 0),'PERIODO':int(e['PERIODO'] or 0),'COD_GEST':e['CODICE_GESTIONALE']})
        if result is not None:
            continue

        ente = db.mdb_enti.find_one({'COD_ENTE' : e['COD_ENTE']})
        cg = db.mdb_codgest_entrate.find_one({'COD_GEST' : e['CODICE_GESTIONALE'],'COD_CATEG':ente['COD_COMPARTO']})

        del ente['_id']
        del cg['_id']
        del cg['COD_CATEG']
    
        ente.update(cg)

        e['COD_GEST'] = e.pop('CODICE_GESTIONALE')
        e['IMPORTO'] = int(e.pop('IMP_USCITE_ATT') or 0)
        e['ANNO'] = int(e['ANNO'] or 0)
        e['PERIODO'] = int(e['PERIODO'] or 0)

        e.update(ente)
    
        bulk.insert(e)
        i += 1

        if i%10000==0:
            bulk.execute()
            bulk = db.mdb_entrate.initialize_unordered_bulk_op()

    try:
        bulk.execute()
    except pymongo.errors.InvalidOperation:
        pass


def creating_uscite_mdb():

    print('CREATING mdb_uscite')

    connection = pymongo.MongoClient(socket)
    db = connection.siope2
    
    db.mdb_uscite.create_index([('COD_ENTE',pymongo.ASCENDING),('ANNO',pymongo.ASCENDING),
                                    ('PERIODO',pymongo.ASCENDING),('COD_GEST',pymongo.ASCENDING)])

    num_documents_csv_uscite = db.csv_uscite.count()
    index = num_documents_csv_uscite//2
    csv_uscite_head = db.csv_uscite.find().limit(index)
    csv_uscite_tail = db.csv_uscite.find().skip(index)

    usciteA = mp.Process(target=creating_uscite_mdb_helper, args=(csv_uscite_head,))
    usciteB = mp.Process(target=creating_uscite_mdb_helper, args=(csv_uscite_tail,))
    usciteA.start()
    usciteB.start()
    usciteA.join()
    usciteB.join()


def creating_uscite_mdb_helper(cursor):

    connection = pymongo.MongoClient(socket)
    db = connection.siope2

    bulk = db.mdb_uscite.initialize_unordered_bulk_op()

    i = 0
    for u in cursor:

        result = db.mdb_uscite.find_one({'COD_ENTE':u['COD_ENTE'],'ANNO':int(u['ANNO'] or 0),'PERIODO':int(u['PERIODO'] or 0),'COD_GEST':u['CODICE_GESTIONALE']})
        if result is not None:
            continue

        ente = db.mdb_enti.find_one({'COD_ENTE' : u['COD_ENTE']})
        cg = db.mdb_codgest_uscite.find_one({'COD_GEST' : u['CODICE_GESTIONALE'],'COD_CATEG':ente['COD_COMPARTO']})

        del ente['_id']
        del cg['_id']
        del cg['COD_CATEG']
    
        ente.update(cg)

        u['COD_GEST'] = u.pop('CODICE_GESTIONALE')
        u['IMPORTO'] = int(u.pop('IMP_USCITE_ATT') or 0)
        u['ANNO'] = int(u['ANNO'] or 0)
        u['PERIODO'] = int(u['PERIODO'] or 0)

        u.update(ente)
    
        bulk.insert(u)
        i += 1

        if i%10000==0:
            bulk.execute()
            bulk = db.mdb_uscite.initialize_unordered_bulk_op()

    try:
        bulk.execute()
    except pymongo.errors.InvalidOperation:
        pass


def build_timeseries():

    print('*** CREATING TIME SERIES *** [Step 3/3]')

    p1 = mp.Process(target=entrate_ts)
    p2 = mp.Process(target=uscite_ts)
    p1.start()
    p2.start()
    p1.join()
    p2.join()


def entrate_ts():

    print('CREATING mdb_entrate_mensili')

    connection = pymongo.MongoClient(socket)
    db = connection.siope2

    mdb_entrate = db.mdb_entrate.find()
    db.mdb_entrate_mensili.drop()
    db.mdb_entrate_mensili.create_index([('_id',pymongo.ASCENDING)])
    bulk = db.mdb_entrate_mensili.initialize_unordered_bulk_op()
    i = 0
    for e in mdb_entrate:

        e['_id'] = str(e['ANNO'])+'/'+str(e['PERIODO'])+'/'+str(e['COD_ENTE'])

        importo = { 'COD_GEST' : e.pop('COD_GEST'),
                    'DESCRIZIONE_CG' : e.pop('DESCRIZIONE_CG'),
                    'IMPORTO' : e.pop('IMPORTO'),
                    'DATA_INIZIO_VALIDITA' : e.pop('DATA_INIZIO_VALIDITA'),
                    'DATA_FINE_VALIDITA' : e.pop('DATA_FINE_VALIDITA')
                    }

        bulk.find({'_id':e['_id']}).upsert().update({'$set':e, '$push':{'IMPORTI':importo}})
        i += 1
        if i%10000==0:
            bulk.execute()
            bulk = db.mdb_entrate_mensili.initialize_unordered_bulk_op()
    try:
        bulk.execute()
    except pymongo.errors.InvalidOperation:
        pass


def uscite_ts():

    print('CREATING mdb_uscite_mensili')

    connection = pymongo.MongoClient(socket)
    db = connection.siope2

    mdb_uscite = db.mdb_uscite.find()
    db.mdb_uscite_mensili.drop()
    db.mdb_uscite_mensili.create_index([('_id',pymongo.ASCENDING)]) 
    bulk = db.mdb_uscite_mensili.initialize_unordered_bulk_op()
    i = 0
    for u in mdb_uscite:
        
        u['_id'] = str(u['ANNO'])+'/'+str(u['PERIODO'])+'/'+str(u['COD_ENTE'])

        importo = { 'COD_GEST' : u.pop('COD_GEST'),
                    'DESCRIZIONE_CG' : u.pop('DESCRIZIONE_CG'),
                    'IMPORTO' : u.pop('IMPORTO'),
                    'DATA_INIZIO_VALIDITA' : u.pop('DATA_INIZIO_VALIDITA'),
                    'DATA_FINE_VALIDITA' : u.pop('DATA_FINE_VALIDITA')
                    }

        bulk.find({'_id':u['_id']}).upsert().update({'$set':u, '$push':{'IMPORTI':importo}})
        i += 1
        if i%10000==0:
            bulk.execute()
            bulk = db.mdb_uscite_mensili.initialize_unordered_bulk_op()
    try:
        bulk.execute()
    except pymongo.errors.InvalidOperation:
        pass


def main():

    print('SCRIPT STARTED AT:')
    sp.call('date +"%T"', shell=True)

    parser = argparse.ArgumentParser(description='Store Siope.it data in MongoDB')
    parser.add_argument('--download=True', action='store_true', dest='download', default=True, help='(DEFAULT) Download data from Siope.it')
    parser.add_argument('--download=False', action='store_false', dest='download', help='Not download data from Siope.it, you must just have them in csvfiles directory')
    parser.add_argument('--host', action='store', dest='host', default='localhost', help='(DEFAULT: localhost) Hostname or IP address where mongod is running')
    parser.add_argument('--port', action='store', dest='port', default='27017', help='(DEFAULT: 27017) Port used by mongod process')
    result = parser.parse_args(sys.argv[1:])
    socket = result.host + ':' + result.port

    table_to_collection(download = result.download)
    build_collection_mdb()
    build_timeseries()

    print('SCRIPT ENDED AT:')
    sp.call('date +"%T"', shell=True)


if __name__ == '__main__':

    main()







