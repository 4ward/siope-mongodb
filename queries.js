var queries = new function() {
    var runTraced = function(command) {
        var before = new Date();
        print (command);
        var result = command();
        var after = new Date()
        var execution_mills = after - before
        print ('Execution time: ' + execution_mills + ' ms')
        return result;
    };

    this.uscitePerEnte = function(anno) {
        return runTraced(function(){
            return db.mdb_uscite_mensili.aggregate([
                {$match : {'ANNO' : anno}},
                {$unwind : '$IMPORTI'},
                {$group : {_id : {'ENTE' : '$DESCR_ENTE'}, 'Totale': {$sum : '$IMPORTI.IMPORTO'}}},
                {$sort : {'Totale' : -1}},
                {$project : {'Totale' : { $divide : ['$Totale', 100000000000]}}},
                {$project : {'Totale Miliardi €' : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}}
                ]);
        })
    };

    this.uscitePerEnteSlow = function(anno) {
        return runTraced(function(){
            return db.mdb_uscite.aggregate([
                {$match : {'ANNO' : anno}},
                {$group : {_id : {'ENTE' : '$DESCR_ENTE'}, 'Totale': {$sum : '$IMPORTO'}}},
                {$sort : {'Totale' : -1}},
                {$project : {'Totale' : { $divide : ['$Totale', 100000000000]}}},
                {$project : {'Totale Miliardi €' : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}}
                ]);
        })
    };

    this.uscitePerEnteMR = function(anno) {
                return runTraced(function(){
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
                })
        };

    this.entratePerEnte = function(anno) {
        return runTraced(function(){
            return db.mdb_entrate_mensili.aggregate([
                {$match : {'ANNO' : anno}},
                {$unwind : '$IMPORTI'},
                {$group : {_id : {'ENTE' : '$DESCR_ENTE'}, 'Totale': {$sum : '$IMPORTI.IMPORTO'}}},
                {$sort : {'Totale' : -1}},
                {$project : {'Totale' : { $divide : ['$Totale', 100000000000]}}},
                {$project : {'Totale Miliardi €' : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}}
                ]);
        })
    };

    this.entratePerEnteSlow = function(anno) {
        return runTraced(function(){
            return db.mdb_entrate.aggregate([
                {$match : {'ANNO' : anno}},
                {$group : {_id : {'ENTE' : '$DESCR_ENTE'}, 'Totale': {$sum : '$IMPORTO'}}},
                {$sort : {'Totale' : -1}},
                {$project : {'Totale' : { $divide : ['$Totale', 100000000000]}}},
                {$project : {'Totale Miliardi €' : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}}
                ]);
        })
    };

    this.entratePerEnteMR = function(anno) {
                return runTraced(function(){
                        return db.mdb_entrate_mensili.mapReduce(
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
                                        out: "entratePerEnteMR"
                                }
                        )
                })
        };

    this.uscitePerRegione = function(anno) {
        return runTraced(function(){
            return db.mdb_uscite_mensili.aggregate([
                {$match : {'ANNO':anno}},
                {$unwind: '$IMPORTI'},
                {$group : {'_id':{'REGIONE':'$DESCR_REGIONE'},'Totale':{$sum:'$IMPORTI.IMPORTO'}}},
                {$sort : {'Totale':-1}},
                {$project : {'Totale' : { $divide : ['$Totale', 100000000000]}}},
                {$project : {'Totale Miliardi €' : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}},
                {$limit:20}
            ]);
        })
    };

    this.entratePerRegione = function(anno) {
        return runTraced(function(){
            return db.mdb_entrate_mensili.aggregate([
                {$match : {'ANNO':anno}},
                {$unwind: '$IMPORTI'},
                {$group : {'_id':{'REGIONE':'$DESCR_REGIONE'},'Totale':{$sum:'$IMPORTI.IMPORTO'}}},
                {$sort : {'Totale':-1}},
                {$project : {'Totale' : { $divide : ['$Totale', 100000000000]}}},
                {$project : {'Totale Miliardi €' : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}},
                {$limit:20}
            ]);
        })
    };

    this.usciteSanitaPerEnte = function(anno) {
        return runTraced(function(){
            return db.mdb_uscite_mensili.aggregate([
                {$match : {'ANNO':anno,'COD_COMPARTO':'SAN'}},
                {$unwind: '$IMPORTI'},
                {$group : {'_id':{'ENTE':'$DESCR_ENTE'},'Totale':{$sum:'$IMPORTI.IMPORTO'}}},
                {$sort : {'Totale':-1}},
                {$project : {'Totale' : { $divide : ['$Totale', 100000000]}}},
                {$project : {'Totale Milioni €' : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}},
                {$limit:20}
            ]);
        })
    };

    this.uscitePerEnteDettaglio = function(anno, descrizioneEnte) {
    return runTraced(function(){
        return db.mdb_uscite_mensili.aggregate([
                {$match : {"ANNO" : anno, "DESCR_ENTE" : descrizioneEnte}},
                {$unwind : "$IMPORTI"},
                {$group : {_id : {"CATEGORIA" : "$IMPORTI.DESCRIZIONE_CG"},"Totale": {$sum : "$IMPORTI.IMPORTO"}}},
                {$sort : {"Totale" : -1}},{$project : {_id :0, "CATEGORIA" : "$_id.CATEGORIA", "Totale" : { $divide : ["$Totale", 100000000]}}},
                {$project : {"CATEGORIA" : 1, "Totale milioni <E2><82><AC>" : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}}]);
        })};

    this.entrateSanitaPerEnte = function(anno) {
        return runTraced(function(){
            return db.mdb_entrate_mensili.aggregate([
                {$match : {'ANNO':anno,'COD_COMPARTO':'SAN'}},
                {$unwind: '$IMPORTI'},
                {$group : {'_id':{'ENTE':'$DESCR_ENTE'},'Totale':{$sum:'$IMPORTI.IMPORTO'}}},
                {$sort : {'Totale':-1}},
                {$project : {'Totale' : { $divide : ['$Totale', 100000000]}}},
                {$project : {'Totale Milioni €' : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}},
                {$limit:20}
            ]);
        })
    };

    this.comparti = function() {
        return runTraced(function(){
            return db.csv_comparti.aggregate([
                {$group : {'_id':{'COD':'$COD_COMPARTO','DESCR':'$DESCRIZIONE_COMPARTO'}}}
            ]);
        })
    };

    this.sottocomparti = function() {
        return runTraced(function(){
            return db.csv_sottocomparti.aggregate([
                {$group : {'_id':{'COD':'$SOTTOCOMPARTO','DESCR':'$DESCRIZIONE'}}}
            ]);
        })
    };

    this.compartiAndSottocomparti = function() {
        return runTraced(function(){
            return db.mdb_enti.aggregate([
                {$group : {'_id':{'CC':'$COD_COMPARTO','DC':'$DESCR_COMPARTO',
                                    'CS':'$COD_SOTTOCOMPARTO','DS':'$DESCR_SOTTOCOMPARTO'}}},
                {$sort:{'_id.CC' : 1}}

            ]);
        })
    };

    this.uscitePerEntePerCategoriaGestionale = function(anno, descrizioneCategoria) {
    return runTraced(function(){
        return db.mdb_uscite_mensili.aggregate([
                {$match : {"ANNO" : anno}},
                {$unwind : "$IMPORTI"},
                {$match : {"IMPORTI.DESCRIZIONE_CG" : descrizioneCategoria}},
                {$group : {_id : {"ENTE" : "$DESCR_ENTE"},"Totale": {$sum : "$IMPORTI.IMPORTO"}}},
                {$sort : {"Totale" : -1}},{$project : {"Totale" : { $divide : ["$Totale", 100000000]}}},
                {$project : {"Totale milioni €" : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}}]);
        })};

    this.uscitePerRegioniPerCategoriaGestionale = function(anno, descrizioneCategoria) {
    return runTraced(function(){
        return db.mdb_uscite_mensili.aggregate([
                {$match : {"ANNO" : anno}},
                {$unwind : "$IMPORTI"},
                {$match : {"IMPORTI.DESCRIZIONE_CG" : descrizioneCategoria}},
                {$group : {_id : {"REGIONE" : "$DESCR_REGIONE"},"Totale": {$sum : "$IMPORTI.IMPORTO"}}},
                {$sort : {"Totale" : -1}},{$project : {"Totale" : { $divide : ["$Totale", 100000000]}}},
                {$project : {"Totale milioni €" : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}}]);
        })};

    this.uscitePerProvincie = function(anno) {
    return runTraced(function(){
        return db.mdb_uscite_mensili.aggregate([
                {$match : {"ANNO" : anno}},
                {$unwind : "$IMPORTI"},
                {$group : {_id : {"Provincia" : "$DESCR_PROVINCIA"},
                "Totale": {$sum : "$IMPORTI.IMPORTO"}}},
                {$sort : {"Totale" : -1}},
                {$project : {"Totale" : { $divide : ["$Totale", 100000000000]}}},
                {$project : {"Totale Miliardi €" : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}}]);
        })};

    this.uscitePerProvinciePerCategoriaGestionale = function(anno, descrizioneCategoria) {
    return runTraced(function(){
        return db.mdb_uscite_mensili.aggregate([
                {$match : {"ANNO" : anno}},
                {$unwind : "$IMPORTI"},
                {$match : {"IMPORTI.DESCRIZIONE_CG" : descrizioneCategoria}},
                {$group : {_id : {"Provincia" : "$DESCR_PROVINCIA"},"Totale": {$sum : "$IMPORTI.IMPORTO"}}},
                {$sort : {"Totale" : -1}},{$project : {"Totale" : { $divide : ["$Totale", 100000000]}}},
                {$project : {"Totale milioni €" : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}}]);
        })};

    this.uscitePerCategoriaGestionale = function(anno) {
    return runTraced(function(){
        return db.mdb_uscite_mensili.aggregate([
                {$match : {"ANNO" : anno}},
                {$unwind : "$IMPORTI"},
                {$group : {_id : {"CATEGORIA" : "$IMPORTI.DESCRIZIONE_CG"},"Totale": {$sum : "$IMPORTI.IMPORTO"}}},
                {$sort : {"Totale" : -1}},{$project : {_id :0, "CATEGORIA" : "$_id.CATEGORIA", "Totale" : { $divide : ["$Totale", 100000000000]}}},
                {$project : {"CATEGORIA" : 1, "Totale Miliardi €" : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}}]);
        })};

    this.uscitePerSottoComparti = function(anno) {
    return runTraced(function(){
        return db.mdb_uscite_mensili.aggregate([
                {$match : {"ANNO" : anno}},
                {$unwind : "$IMPORTI"},
                {$group :{_id : {"SOTTOCOMPARTO" : "$DESCR_SOTTOCOMPARTO"},
                "Totale": {$sum : "$IMPORTI.IMPORTO"}}},
                {$sort : {"Totale" : -1}},
                {$project : {"Totale" : { $divide : ["$Totale", 100000000000]}}},
                {$project : {"Totale Miliardi €" : {$divide:[{$subtract:[{$multiply:['$Totale',100]},
                {$mod:[{$multiply:['$Totale',100]}, 1]}]},100]}}}]);
        })};
};
