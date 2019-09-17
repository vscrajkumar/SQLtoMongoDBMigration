var mysql = require('mysql');
var MongoClient = require('mongodb').MongoClient;

function getTABLESfromSQL(Mysql_Connectionnection, callback) {
    Mysql_Connectionnection.query("show full tables where Table_Type = 'BASE TABLE';", function(error, results, fields) {
        if (error) {
            callback(error);
        } else {
            var tables = [];
            results.forEach(function (row) {
                for (var key in row) {
                    if (row.hasOwnProperty(key)) {
                        if(key.startsWith('Tables_in')) {
                            tables.push(row[key]);
                        }
                    }
                }
            });
            callback(null, tables);
        }
    });
}

function CollectionTable(Mysql_Connectionnection, tableName, mongoCollection, callback) {
    var sql = 'SELECT * FROM ' + tableName + ';';
    Mysql_Connectionnection.query(sql, function (error, results, fields) {
        if (error) {
            callback(error);
        } else {
            if (results.length > 0) {
                mongoCollection.insertMany(results, {}, function (error) {
                    if (error) {
                        callback(error);
                    } else {
                        callback(null);
                    }
                });
            } else {
                callback(null);
            }
        }
    });
}

MongoClient.connect("mongodb://localhost:27017/importedDb", function (error, db) {
    if (error) throw error;
    var Mysql_Connection = mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: 'root',
        port: 8889,
        database: 'my_database'
    });
    Mysql_Connection.connect();
    var jobs = 0;
    getTABLESfromSQL(Mysql_Connection, function(error, tables) {
        tables.forEach(function(table) {
            var collection = db.collection(table);
            ++jobs;
            CollectionTable(Mysql_Connection, table, collection, function(error) {
                if (error) throw error;
                --jobs;
            });
        })
    });
    var interval = setInterval(function() {
        if(jobs<=0) {
            clearInterval(interval);
            db.close();
            Mysql_Connection.end();
        }
    }, 300);
});
