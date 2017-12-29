/*jshint esversion: 6 */
/*jslint node: true */
"use strict";

const mysql = require('mysql'),
      path = require('path'),
      firstline = require('firstline'),
      async = require('async'),
      _ = require('lodash');

module.exports = class MySQL {
    /**
     * Constructor
     * Creates a connection pool for calls to be made
     * @param  {string} connection Mysql connection string to mysql db
     * @param  {object} connection Mysql connection object to mysql db
     */
    constructor (connection) {
        if(!_.isString(connection) && !_.isObject(connection)) throw new Error('Mysql Class - Please provide a connect string or connection object https://github.com/mysqljs/mysql#introduction');
        this.pool = mysql.createPool(connection);
        this.debug = false;
        this.log = console.log;
    }
    
    /**
     * Set Debug
     */
    setDebug(winston = null) {
        this.debug = true;
        if(winston) this.log = winston.debug;
    }
    
    /**
     * getConnection
     * Calls the pool to create a sql connection
     * @return {obj} db - a mysql connection
     */
    getConnection() {
        const self = this;
        return new Promise(function(resolve,reject){
            self.pool.getConnection(function(err, db) {
                if(err) return reject(err);
                resolve(db);
            });
        });
    }
    
    /**
     * query
     * @param  {string} command SQL command
     * @return {promise}        Returns the result of the sql command in a promise
     */
    query(command) {
        const self = this;
        return new Promise(function(resolve,reject){
            self.getConnection()
            .then(function(db){
                db.query(command,function(err,response){
                    if(err && self.debug) self.log(err);
                    if(err) return reject(err);
                    db.release();
                    resolve(response);
                });
            }).catch(reject);
        });
    }
    
    /**
     * table exists
     * @param  {string} table MySql table to check for existance in the db
     * @return {promsie}      If table exists resolves, if error rejects
     */
    tableExists(table) {
        const self = this;
        return new Promise(function(resolve,reject){
            self.query(`SHOW TABLES LIKE '${table}'`)
            .then(function(results){
                if(results.length && self.debug) self.log(`${table} found`);
                if(results.length) return resolve();
                if(!results.length && self.debug) self.log(`${table} NOT found`);
                reject();
            }).catch(reject);
        });
    }
    
    /**
     * getHeaders
     * @param  {string} filepath  path and name of the file
     * @param  {string} delimiter delimiter for parsing each file
     * @return {array}            each of the headers in an array
     */
    getHeaders(filepath, delimiter=",") {
        const self = this;
        return new Promise(function(resolve,reject){
            if(!filepath) return reject(new Error("Missing filepath parameter"));
            firstline(filepath)
            .then(function(line){
                if(self.debug) self.log(`Finding Headers using delimiter: "${delimiter}". Headers found:`, line.split(delimiter));
                resolve(line.split(delimiter));
            }).catch(reject);
        });
    }
    
    /**
     * createStagingTable
     * Check if table EXISTS
     * Create connection from Pool
     * Drop staging table if exists
     * Create a staging table named after the intended table ex: table_staging
     * @param  {[type]} table name of the table to copy
     * @return {[type]}       creates a new table like the referenced table
     */
    createStagingTable (table) {
        const self = this;
        return new Promise(function(resolve,reject){
            if(!table) return reject(new Error('Missing Table Name on createTable method for MySQL Class'));
            
            self.tableExists(table)
            .then(function(){
                if(self.debug) self.log(`${table} found to copy from. Creating staging table.`);
                self.getConnection()
                .then(function(db){
                    db.query(`DROP TABLE IF EXISTS ${table}_staging`,function(err, results){
                        if(err) return reject(new Error('Error dropping existing staging table in MySQL Class', err.message));
                        db.query(`CREATE TABLE ${table}_staging LIKE ${table}`, function(err, results){
                            if(err) return reject(new Error(`Creating a new Staging table like ${table} in MySQL Class`, err.message));
                            if(self.debug) self.log(`${table}_staging created by copying ${table}`);
                            resolve(table + '_staging');
                        });
                    });
                }).catch(reject);
            })
            .catch(function(err){
                return reject(new Error('Table does not exist to copy for importing file. Ensure table is already defined OR use importFileAndCreateTable() method, in MySQL Class'));
            });
            
        });
    }
    
    /**
     * createNewTable
     * @param  {string} name    Name of table to be creatws
     * @param  {array}  headers List of header names
     * @param  {bool}   overwrite Overwrite if an existing table is found
     * @return {string}         Name of new table
     */
    createNewTable(name, headers, overwrite=false) {
        const self = this;
        
        return new Promise(function(resolve,reject){
            if(!name) return reject(new Error("Must provide a name for the new table in createNewTable()"));
            if(!headers && !_.isArray(headers) && !headers.length) return reject(new Error('No headers provided to create new table or not passed as an array in MySQL Class'));
            let headerString = headers.map(function(header){ return header + ' VARCHAR(1000)';}).join(', ');
            
            if(self.debug) self.log(`Creating new table called ${name}. Headers string for CREATE command: ${headerString}`);
            
            let create = function() {
                return new Promise(function(resolve,reject){
                    self.query(`CREATE TABLE ${name} (${headerString})`)
                    .then(function(results){
                        if(self.debug) self.log(`${name} successfully created`);
                        resolve(name);
                    }).catch(function(err){
                        if(err) return reject(new Error('Error creating new table in MySQL Class' + err));
                    });
                });
            };
            
            let drop = function() {
                return new Promise(function(resolve,reject){
                    if(self.debug) self.log(`${name} already exists, checking for overwrite param`);
                    if(!overwrite) return reject(new Error('Error creating new table. One already exists with the name provided and overwrite parameter was not set to TRUE'));
                    if(self.debug) self.log(`Overwritng ${name} with new table definition`);
                    self.query(`DROP TABLE IF EXISTS ${name}`)
                    .then(create)
                    .then(resolve)
                    .catch(reject);
                });
            };
            
            self.tableExists(name)
            .then(drop, create)
            .then(resolve)
            .catch(function(err){
                reject(new Error('Error creating new table in MySQL Class Method createNewTable()' + err));
            });
            
        });
    }
    
    /**
     * swapTables
     * Rename existing "real" table to table_drop
     * Rename new table to orignal table name
     * Drop old table
     * Limits amount of time the table is offline
     * @param  {string} table  Name of target table
     * @return {promsie}       Resolves when complete
     */
    swapTables(table) {
        const self = this;
        
        return new Promise(function(resolve,reject){
            self.tableExists(table)
            .then(function(){
                self.getConnection()
                .then(function(db){
                    if(self.debug) self.log(`Swapping ${table}_staging with ${table}`);
                    db.query(`RENAME TABLE ${table} TO ${table}_drop`,function(err){
                        if(err) return reject(err);
                        db.query(`RENAME TABLE ${table}_staging TO ${table}`,function(err){
                            if(err) return reject(err);
                            db.query(`DROP TABLE ${table}_drop`,function(err){
                                if(err) return reject(err);
                                if(self.debug) self.log(`Old ${table} dropped and ${table}_staging renamed to ${table}. Swap complete`);
                                resolve();
                            });
                        });
                    });
                }).catch(reject);
            },function(){
                if(self.debug) self.log(`${table} doesn't exist so renaming ${table}_staging to ${table}`);
                self.query(`RENAME TABLE ${table}_staging TO ${table}`)
                .then(function(response){
                    resolve();
                }).catch(reject);
            }).catch(reject);
            
        });
    }
    
    /**
     * importFileToTable
     * Imports a delimited text file into an existing table
     * An existing table is required
     * First creates a staging version of the table, loads the data, then swaps the two tables
     * @param  {string} filepath        Path of file to target file
     * @param  {string} table           Table where data will be loaded. If none is provided, falls back to name of file
     * @param  {array}  [headers]       Headers of the file to import, will import null to tables that are in the table but not passed in this array. Blank will import all headers in the file.
     * @param  {String} [delimiter=","] Delimiter in file for parsing header, defaults to ","
     * @param  {String} [quotes=""]     Character wrapping field values
     * @param  {String} [newline="\n"]  Character terminating lines of a each line in the file
     * @return {promsie}                resovles promsie with number of rows imported
     */
    importFileToTable({filepath, table = "", headers = [], delimiter = ",", quotes = '', newline = "\n"}) {
        const self = this;
        
        return new Promise(function(resolve,reject){
            if(!filepath) return reject(new Error("Missing File Path"));
            if(!table) table = path.parse(filepath).name;
            
            self.tableExists(table)
            .then(function(){
                return self.createStagingTable(table);
            }, function(){
                reject(new Error(`No matching table found for ${table}`));
            })
            .then(function(){
                return self.query(`LOAD DATA LOCAL INFILE '${filepath}' INTO TABLE ${table}_staging FIELDS TERMINATED BY '${delimiter}' ENCLOSED BY '${quotes}' LINES TERMINATED BY '${newline}' (${headers.join(', ')})`);
            })
            .then(function(results){
                return new Promise(function(resolve,reject){
                    if(self.debug) self.log('File loaded with results:', results);
                    self.swapTables(table)
                    .then(function(){
                        if(self.debug) self.log('Tables swapped, data fully loaded');
                        resolve(results.affectedRows);
                    }).catch(reject);
                });
            })
            .then(resolve)
            .catch(reject);
        });
    }
    
    /**
     * importFileAndCreateTable
     * Creates a new table and Imports a delimited text file into that table
     * @param  {string} filepath        Path of file to target file
     * @param  {string} table           Table where data will be loaded. If none is provided, falls back to name of file
     * @param  {array}  [headers]       Headers of the file to import, will also use this array to create fields in new table. If none is provided, will lookup headers in the file and use those.
     * @param  {String} [delimiter=","] Delimiter in file for parsing header, defaults to ","
     * @param  {String} [quotes=""]     Character wrapping field values
     * @param  {String} [newline="\n"]  Character terminating lines of a each line in the file
     * @return {promsie}                resovles promsie with number of rows imported
     */
    importFileAndCreateTable({filepath, table = "", overwrite = false, headers = [], delimiter = ",", quotes = '', newline = "\n"}) {
        const self = this;
        
        return new Promise(function(resolve,reject){
            if(!filepath) return reject(new Error("Missing File Path"));
            if(!table) table = path.parse(filepath).name;
            
            new Promise(function(resolve, reject){
                if(headers.length) return resolve(headers);
                self.getHeaders(filepath, delimiter).then(resolve).catch(reject);
            })
            .then(function(headers){
                if(self.debug) self.log(`About to create new table ${table}`);
                return self.createNewTable(table, headers, overwrite);
            })
            .then(function(){
                if(self.debug) self.log(`About to load data from ${filepath} into ${table}`);
                return self.query(`LOAD DATA LOCAL INFILE '${filepath}' INTO TABLE ${table} FIELDS TERMINATED BY '${delimiter}' ENCLOSED BY '${quotes}' LINES TERMINATED BY '${newline}' (${headers.join(', ')})`);
            })
            .then(function(results){
                if(self.debug) self.log(`Loaded file, ${results.affectedRows} rows affected`);
                return results.affectedRows;
            })
            .then(resolve)
            .catch(reject);
        });
    }
    
    exportFile(file, table, headers = [], delimiter = ",") {
        return this.query(`SELECT ${!headers.length ? headers.join(',') : '*'} INTO OUTFILE ${file} FIELDS TERMINATED BY ${delimiter} FROM ${table}`);
    }
    
    mergeFiles(files, output = './tmp/merged.csv') {
        const self = this;
        
        return new Promise(function(resolve,reject){
            if(!files || !_.isArray(files) || !files.length) return reject("MySQL Class // MergeFiles - The 'files' parameter (1) is missing, or misformatted. Must be an array of objects");
            let tables = [];
            
            async.each(files, function(file, next){
                let fileObj = !_.isObject(file) && _.isString(file) ? {'path': file} : file;
                if(!fileObj.path) return reject("MySQL Class / mergeFiles Method - The file" + file + "is missing the 'path' property");
                if(!fileObj.headers || !fileObj.headers.length) fileObj.headers = [];
                if(!fileObj.delimiter) fileObj.delimiter = ",";
                if(!fileObj.table) fileObj.table = path.parse(file).name;
                
                tables.push(fileObj.table);
                
                self.importFile(fileObj.path, fileObj.table, fileObj.headers, fileObj.delimiter)
                .then(next).catch(next);
                
            }, function(err){
                if(err) return reject(err);
                
                self.getConnection()
                .then(function(db){
                    db.query(`CREATE VIEW v AS SELECT * FROM  `);
                });
                self.query(`SELECT * INTO OUTFILE ${output} FIELDS TERMINATED BY ',' FROM ${tables}`);
            });
        });
    }
    
};