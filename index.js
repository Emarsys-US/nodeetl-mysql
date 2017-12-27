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
            self.query(`SELECT 1 FROM ${table} LIMIT 1`)
            .then(function(){
                resolve();
            })
            .catch(function(err){
                reject();
            });
        });
    }
    
    /**
     * getHeaders
     * @param  {string} filepath  path and name of the file
     * @param  {string} delimiter delimiter for parsing each file
     * @return {array}            each of the headers in an array
     */
    getHeaders(filepath, delimiter=",") {
        return new Promise(function(resolve,reject){
            firstline(filepath)
            .then(function(line){
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

            self.getConnection()
            .then(function(db){
                db.query(`DROP TABLE IF EXISTS ${table}_staging`,function(err, results){
                    if(err) return reject(new Error('Error dropping existing staging table in MySQL Class', err.message));
                    db.query(`CREATE TABLE ${table}_staging LIKE ${table}`, function(err, results){
                        if(err) return reject(new Error(`Creating a new Staging table like ${table} in MySQL Class`, err.message));
                        resolve(table + '_staging');
                    });
                });
            }).catch(reject);
        });
    }
    
    /**
     * createNewTable
     * @param  {string} name    Name of table to be creatws
     * @param  {array}  headers List of header names
     * @return {string}         Name of new table
     */
    createNewTable(name, headers) {
        const self = this;
        
        return new Promise(function(resolve,reject){
            if(!headers && !_.isArray(headers) && !headers.length) return reject('No headers provided to create new table or not passed as an array in MySQL Class');
            let headerString = headers.map(function(header){ return header + ' VARCHAR(255)';}).join(', ');

            self.query(`CREATE TABLE ${name} (${headerString})`)
            .then(function(results){
                resolve(name);
            }).catch(function(err){
                if(err) return reject(new Error('Error creating new table with provided headers for MySQL Class' + err));
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
                    db.query(`RENAME TABLE ${table} TO ${table}_drop`,function(err){
                        if(err) return reject(err);
                        db.query(`RENAME TABLE ${table}_staging TO ${table}`,function(err){
                            if(err) return reject(err);
                            db.query(`DROP TABLE ${table}_drop`,function(err){
                                if(err) return reject(err);
                                resolve();
                            });
                        });
                    });
                }).catch(reject);
            },function(){
                self.query(`RENAME TABLE ${table}_staging TO ${table}`)
                .then(function(err, response){
                    if(err) return reject(err);
                    resolve();
                });
            }).catch(reject);
            
        });
    }
    
};