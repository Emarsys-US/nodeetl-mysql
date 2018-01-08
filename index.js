/*jshint esversion: 6 */
/*jslint node: true */
"use strict";

const mysql = require('mysql'),
      fs = require('fs'),
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
        if(!_.isString(connection) && !_.isObject(connection)) throw new Error('Mysql Class / Constructor - Please provide a connect string or connection object https://github.com/mysqljs/mysql#introduction');
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
     * Returns TRUE if the first specified array contains all elements
     * from the second one. FALSE otherwise.
     * Stolen from: https://github.com/lodash/lodash/issues/1743
     * @param {array} superset
     * @param {array} subset
     * @returns {boolean}
    */
    arrayContainsArray (superset, subset) {
        return subset.every(function (value) {
            return (superset.indexOf(value) >= 0);
        });
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
    query(command, ...variables) {
        const self = this;
        return new Promise(function(resolve,reject){
            self.getConnection()
            .then(function(db){
                let sql = db.format(command, [...variables]);
                db.query(sql, function(err,response){
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
     * dropTable
     * Drop table or tables
     * @param  {string | array} table Name of table or tables to drop
     * @return {Promise}              Promise resolves if successful
     */
    dropTable(table){
        const self = this;
        return new Promise(function(resolve,reject){
            if(!table || !table.length) return reject(new Error("MySQL Class / dropTable Method - Missing Table Parameter. Must be a string or array"));
            let tables = _.concat([],table);
            async.each(tables,function(table,next){
                self.query(`DROP TABLE IF EXISTS ${table}`)
                .then(function(){
                    next();
                }).catch(next);
            },function(err){
                if(err) return reject(err);
                resolve();
            });
        });
    }
    
    /**
     * addIndex
     * @param {string} table Name of table to add index to
     * @param {string | array} index Field(s) to index
     */
    addIndex(table,index) {
        const self = this;
        
        return new Promise(function(resolve,reject){
            if(!table) return reject(new Error("MySQL Class / addIndex Method - Missing Table Parameter. Must be a string of table you want to add index(es) to."));
            if(!index) return reject(new Error("MySQL Class / addIndex Method - Missing Index Parameter. Must be a string or array of headers you want to create index(es) for in the table."));
            
            async.each(_.concat([],index),function(ind,next) {
                self.query(`CREATE INDEX ${ind} on ${table} (${ind}) USING BTREE`)
                .then(function(){
                    next();
                }).catch(next);
            },function(err){
                if(err) return reject(err);
                resolve();
            });
        });
    }
    
    
    /**
     * getFileHeaders
     * @param  {string} filepath  path and name of the file
     * @param  {string} delimiter delimiter for parsing each file
     * @return {array}            each of the headers in an array
     */
    getFileHeaders(filepath, delimiter=",") {
        const self = this;
        return new Promise(function(resolve,reject){
            if(!filepath) return reject(new Error("MySQL Class / getFileHeaders Method - Missing filepath parameter"));
            firstline(filepath)
            .then(function(line){
                if(self.debug) self.log(`Finding Headers using delimiter: "${delimiter}". Headers found:`, line.split(delimiter));
                resolve(line.split(delimiter));
            }).catch(reject);
        });
    }
    
    /**
     * getTableHeaders
     * @param  {string} table    Name of table
     * @return {Promsie | array} List of fields in the table
     */
    getTableHeaders(table) {
        const self = this;
        return new Promise(function(resolve,reject){
            self.query(`SELECT GROUP_CONCAT(CONCAT("'",COLUMN_NAME,"'")) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '${table}' ORDER BY ORDINAL_POSITION;`)
            .then(function(response){
                let string = response[0]['GROUP_CONCAT(CONCAT("\'",COLUMN_NAME,"\'"))'];
                let headers = string.split(',').map(function(header){
                    return _.trim(header, "'");
                });
                resolve(headers);
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
            if(!table) return reject(new Error('MySQL Class / createStagingTable Method - Missing Table Name property'));
            
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
                return reject(new Error('MySQL Class / createStagingTable Method - Table does not exist to copy for importing file. Ensure table is already defined OR use importFileAndCreateTable() method'));
            });
            
        });
    }
    
    /**
     * createNewTable
     * @param  {string} table   Name of table to be creatws
     * @param  {array}  headers List of header names
     * @param  {array}  index   Which headers should be indexed
     * @param  {bool}   prependHeaders If headers should have the table name prepended
     * @param  {bool}   overwrite Overwrite if an existing table is found
     * @return {string}         Name of new table
     */
    createNewTable({table, headers, index = null, prependHeaders = false, overwrite=false}) {
        const self = this;
        
        return new Promise(function(resolve,reject){
            if(!table) return reject(new Error("MySQL Class / createNewTable Method - Must provide a name for the new table"));
            if(!headers && !_.isArray(headers) && !headers.length) return reject(new Error('MySQL Class / createNewTable Method - No headers provided to create new table or not passed as an array'));
            if(index && self.arrayContainsArray(_.concat([],index), headers)) return reject(new Error('MySQL Class / createNewTable Method - Cannot create table. Index must be included in the headers array to avoid an error.'));
            if(prependHeaders && index) index = _.map(_.concat([],index),function(ind){ return table + "_" + _.camelCase(ind);});
            
            let headerString = headers.map(function(header){
                if(prependHeaders) header = table + '_' + _.camelCase(header);
                return header + ' VARCHAR(1000)';
            }).join(', ');
            
            if(self.debug) self.log(`Creating new table called ${table}. Headers string for CREATE command: ${headerString}`);
            
            let create = function() {
                return new Promise(function(resolve,reject){
                    self.query(`CREATE TABLE ${table} (${headerString})`)
                    .then(function(results){
                        if(!index) return;
                        return self.addIndex(table,index);
                    })
                    .then(function(results){
                        if(self.debug) self.log(`Index ${index} on ${table} successfully created`);
                        resolve(table);
                    }).catch(function(err){
                        if(err) return reject(new Error('Error creating new table in MySQL Class' + err));
                    });
                });
            };
            
            let drop = function() {
                return new Promise(function(resolve,reject){
                    if(self.debug) self.log(`${table} already exists, checking for overwrite param`);
                    if(!overwrite) return reject(new Error('Error creating new table. One already exists with the table provided and overwrite parameter was not set to TRUE'));
                    if(self.debug) self.log(`Overwritng ${table} with new table definition`);
                    self.query(`DROP TABLE IF EXISTS ${table}`)
                    .then(create)
                    .then(resolve)
                    .catch(reject);
                });
            };
            
            self.tableExists(table)
            .then(drop, create)
            .then(resolve)
            .catch(function(err){
                reject(new Error('MySQL Class / createNewTable Method - Error creating new table in because: ' + err));
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
            if(!filepath) return reject(new Error("MySQL Class / importFileToTable Method - Missing File Path"));
            if(!table) table = path.parse(filepath).name;
            
            self.tableExists(table)
            .then(function(){
                return self.createStagingTable(table);
            }, function(){
                reject(new Error(`No matching table found for ${table}`));
            })
            .then(function(){
                return self.query(`LOAD DATA LOCAL INFILE '${filepath}' INTO TABLE ${table}_staging FIELDS TERMINATED BY '${delimiter}' ENCLOSED BY '${quotes}' LINES TERMINATED BY '${newline}' IGNORE 1 LINES (${headers.join(', ')})`);
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
    importFileAndCreateTable({filepath, table = "", overwrite = false, index = null, headers = [], prependHeaders = false, delimiter = ",", quotes = '', newline = "\n"}) {
        const self = this;
        
        return new Promise(function(resolve,reject){
            if(!filepath) return reject(new Error("MySQL Class / importFileAndCreateTable Method - Missing File Path"));
            if(!fs.existsSync(filepath)) return reject(new Error("MySQL Class / importFileAndCreateTable Method - Cannot find file at " + filepath));
            if(!table) table = path.parse(filepath).name;
            
            new Promise(function(resolve, reject){
                if(headers.length) return resolve(headers);
                self.getFileHeaders(filepath, delimiter).then(resolve).catch(reject);
            })
            .then(function(headers){
                if(self.debug) self.log(`About to create new table ${table}`);
                return self.createNewTable({filepath: filepath, table: table, overwrite: overwrite, index:index, headers:headers, prependHeaders:prependHeaders});
            })
            .then(function(){
                if(self.debug) self.log(`About to load data from ${filepath} into ${table}`);
                return self.query(`LOAD DATA LOCAL INFILE '${filepath}' INTO TABLE ${table} FIELDS TERMINATED BY '${delimiter}' ENCLOSED BY '${quotes}' LINES TERMINATED BY '${newline}' IGNORE 1 LINES ${headers? '('+ headers.join(', ') +')' : ''}`);
            })
            .then(function(results){
                if(self.debug) self.log(`Loaded file, ${results.affectedRows} rows affected`);
                return results.affectedRows;
            })
            .then(resolve)
            .catch(reject);
        });
    }
    
    /**
     * exportFileFromTable
     * Takes all data from a table and exports it to file
     * @param  {string} filepath        Location of where the output file will export to.
     * @param  {String} [table=""]      Name of the table to target, defaults to name of file
     * @param  {Array}  [headers=[]]    Array of headers to export from table, otherwise exports all
     * @param  {String} [delimiter=","] Delimiter of output file
     * @param  {String} [quotes='"']    Optionally wrap values in quotes in output file
     * @param  {String} [newline="n"}]  Newline character in ourput file
     * @return {Promise}                Resovles true, exports file to disk
     */
    exportFileFromTable({filepath, table = "", headers = [], delimiter = ",", quotes = '"', newline = "\n"}) {
        const self = this;
        
        return new Promise(function(resolve,reject){
            if(!filepath) return reject(new Error("Missing File Path"));
            if(!table) table = path.parse(filepath).name;
            
            self.tableExists(table)
            .then(function(){
                return new Promise(function(resolve,reject){
                    fs.unlink(filepath,function(err){
                        resolve();
                    });
                });
            }, function(){
                reject(new Error(`exportFile - No matching table found for ${table}`));
            })
            .then(function(){
                if(headers.length) return headers;
                return self.getTableHeaders(table);
            })
            .then(function(headers) {
                // Needs to copy headers here to include them in the CSV export (rolling eyes emoji)
                let sql = `SELECT ${headers.map(function(header){ return '\"' + header + '\"'; }).join(', ')}`;
                sql += ` UNION ALL`;
                sql += ` SELECT ?? FROM ${table} INTO OUTFILE '${filepath}' FIELDS TERMINATED BY '${delimiter}' OPTIONALLY ENCLOSED BY '${quotes}' LINES TERMINATED BY '${newline}'`;
                return self.query(sql, headers);
            })
            .then(function(results){
                resolve(results.affectedRows);
            })
            .catch(reject);
        });
    }
    
    /**
     * Merge Multiple Files Together
     * @param  {array}  files  Collection of file objects that describe the files being imported. Same as importFileAndCreateTable.
     * @param  {object} merge  Object describing how tables map together {"table.field": "othertable".field}
     * @param  {String} output Output location of the final file
     * @return {Promise}       Resolved promised when done with number of rows exported
     */
    mergeFiles(files, merge, output) {
        const self = this;
        if(self.debug) self.log(`Starting merge of ${_.size(files)} files`);
        
        return new Promise(function(resolve,reject){
            if(!files || !_.isArray(files) || !files.length) return reject("MySQL Class // MergeFiles - The 'files' parameter (1) is missing, or misformatted. Must be an array of objects");
            if(!merge) return reject("MySQL Class // MergeFiles - The 'merge' parameter (2) is missing, or misformatted. Must be an object");
            if(!output) return reject("MySQL Class // MergeFiles - The 'output' parameter (3) is missing, or misformatted. Must be a string");
            let tables = [],
                mergeTable = null;
            
            // Check file properties
            // Create tables array
            _.each(files,function(file){
                if(!file.filepath) return reject("MySQL Class / mergeFiles Method - The file" + file.toString() + "is missing the 'path' property");
                if(!file.index) return reject("MySQL Class / mergeFiles Method - The file" + file.toString() + "is missing the 'index' property");
                if(!file.headers || !file.headers.length) file.headers = [];
                if(!file.delimiter) file.delimiter = ",";
                if(!file.table) file.table = path.parse(file).name;
                file.prependHeaders = true;
                
                tables.push(file.table);
            });
            
            // Import each file into its own table.
            // Prepend table headers with the table name so no conflicts arise
            let importFiles = function(){
                return new Promise(function(resolve,reject){
                    async.each(files, function(file, next){
                        self.importFileAndCreateTable(file)
                        .then(function(name){
                            next();
                        }).catch(next);
                    }, function(err){
                        if(err) return reject(err);
                        if(self.debug) self.log(`Imported all files into tables`);
                        resolve();
                    });
                });
            };
            
            // Create merge statement looping over merge object to map tables together
            let joinIntoTable = function() {
                return new Promise(function(resolve,reject){
                    let merges = _.toPairs(merge);
                    mergeTable = 'merge_' + Math.random().toString(36).substring(7);
                    
                    let join = `CREATE TABLE ${mergeTable} SELECT * FROM ${tables.join(', ')}`;
                    
                    _.each(merges, function(merge, i){
                        join += (i === 0) ? ' WHERE' : ' AND';
                        join += ` ${merge[0]} = ${merge[1]}`;
                    });
                    
                    if(self.debug) self.log(`Merge statement built: ${join}`);
                    
                    self.query(join)
                    .then(function(){
                        if(self.debug) self.log(`Merged tables into new table ${mergeTable}`);
                        resolve();
                    }).catch(reject);
                });
            };
            
            // Export the merge table to a file
            let exportToFile = function() {
                return self.exportFileFromTable({filepath: output, table: mergeTable});
            };
            
            // Drop all tables needed for merge
            let dropMergeTables = function(rowsExported) {
                return new Promise(function(resolve,reject){
                    if(self.debug) self.log(`Dropping file tables ${tables} and the merge table ${mergeTable}`);
                    self.dropTable(_.concat(mergeTable,tables))
                    .then(function(){
                        resolve(rowsExported);
                    }).catch(reject);
                });
            };
            
            // Merge Files Flow
            importFiles()
            .then(joinIntoTable)
            .then(exportToFile)
            .then(dropMergeTables)
            .then(resolve)
            .catch(reject);
        });
    }
    
};