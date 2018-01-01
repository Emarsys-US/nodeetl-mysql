/*jshint esversion: 6 */
/*jslint node: true */

const mocha = require('mocha'),
      chai = require('chai'),
      chaiAsPromised = require("chai-as-promised"),
      fs = require('fs'),
      MySQL = require('../index.js');
       
chai.use(chaiAsPromised);
chai.should(); 
  
const mysql = new MySQL({
  host     : 'localhost',
  user     : 'root',
  database : 'nodeetl-mysql'
});
mysql.setDebug();


describe('Table Methods', function(){
    before(function(done){
        mysql.query('CREATE TABLE IF NOT EXISTS test (email VARCHAR(255), first VARCHAR(255), last VARCHAR(255))')
        .then(function(){
            done();
        }).catch(done);
    });
    
    after(function(done){
        mysql.query('DROP TABLE IF EXISTS test')
        .then(function(){
            return mysql.query('DROP TABLE IF EXISTS test2');
        })
        .then(function(){
            return mysql.query('DROP TABLE IF EXISTS test3');
        })
        .then(function(){
            done();
        })
        .catch(done);
    });

     
    describe('Funcions', function(){
        it('Creates Connections', function(){
            return mysql.getConnection().should.eventually.be.fulfilled;
        });
        
        it('Runs Commands', function(){
            return mysql.query('SELECT 1 FROM test LIMIT 1').should.eventually.be.fulfilled;
        });

    });

    describe('Utilities', function(){
        it('Checks if a Table Exists',function(){
            return mysql.tableExists('test').should.eventually.be.fulfilled;
        });
        
        it('Gets Headers from File', function(){
            return mysql.getHeaders('./test/data.csv').should.eventually.deep.equal(['email', 'first', 'last']);
        });
    });
    
    describe('Creates Tables', function(){
        it('Creates Staging Table from Existing Table', function(){
            return mysql.createStagingTable('test').should.eventually.equal('test_staging');
        });
        
        it('Creates a New Table from Headers', function(){
            return mysql.createNewTable('test2', ['email', 'first', 'last']).should.eventually.equal('test2');
        });
        
        it('Overwrites an Existing table and Creates a New One', function(){
            return mysql.createNewTable('test2', ['email', 'first', 'last'], null, true).should.eventually.equal('test2');
        });
        
        it('Reject When Trying to Create a Staging Table from a Non-Exsting Table', function(){
            return mysql.createStagingTable('asdfasdf').should.eventually.be.rejected;
        });
        
        it('Reject When Trying to Create a New Table and One With That Name Already Exists', function(){
            return mysql.createNewTable('test').should.eventually.be.rejected;
        });
    });
    
    describe('Renames Tables', function(){
        it('Swaps Tables with Existing Table', function(){
            return mysql.swapTables('test').should.eventually.be.fulfilled;
        });
        
        it('Renames Table if no Existing Table is Present', function(done){
            mysql.query(`CREATE TABLE test3_staging LIKE test2`)
            .then(function(){
                return mysql.swapTables('test3');
            })
            .then(function(){
                done();
            }).catch(done);
        });
    });
});

describe('File Methods', function(){
    before(function(done){
        mysql.query('CREATE TABLE IF NOT EXISTS data (email VARCHAR(255), first VARCHAR(255), last VARCHAR(255))')
        .then(function(){
            return mysql.query('DROP TABLE IF EXISTS datamerge1');
        })
        .then(function(){
            return mysql.query('DROP TABLE IF EXISTS datamerge2');
        })
        .then(function(){
            done();
        }).catch(done);
    });
    
    after(function(done){
        mysql.query('DROP TABLE IF EXISTS data')
        .then(function(){
            return mysql.query('DROP TABLE IF EXISTS data2');
        })
        // .then(function(){
        //     return mysql.query('DROP TABLE IF EXISTS datamerge1');
        // })
        // .then(function(){
        //     return mysql.query('DROP TABLE IF EXISTS datamerge2');
        // })
        .then(function(){
            return new Promise(function(resolve,reject){
                fs.unlink('./test/export.csv', function(err){
                    if(err) return reject();
                    resolve();
                });
            });
        }).then(function(){ done();}).catch(done);
    });
    
    describe('Imports', function(){
        it('Imports File to an Existing Table', function(){
            return mysql.importFileToTable({filepath: './test/data.csv', quotes: '"'}).should.eventually.equal(4);
        });
        
        it('Creates a New Table Using a File and Imports File', function(){
            return mysql.importFileAndCreateTable({filepath: './test/data.csv', table: 'data2', headers: ['email', 'first'], quotes: '"'}).should.eventually.equal(4);
        });
    });
    
    describe('Exports', function(){
        it('Exports a table to a file', function(done){
            mysql.exportFileFromTable({filepath: __dirname + '/export.csv', table: 'data2'})
            .then(function(results){
                results.should.equal(4);
                fs.existsSync(__dirname + '/export.csv').should.equal(true);
                done();
            }).catch(done);
        });
    });
    
    describe('Merging', function(){
        it('Merges Multiple Files into a Single File', function(done){
            let files = [
                {
                    filepath: './test/data.csv',
                    table: 'datamerge1',
                    index: 'email'
                },
                {
                    filepath: './test/data2.csv',
                    table: 'datamerge2',
                    index: 'email',
                }
            ];
            
            mysql.mergeFiles(files, __dirname + 'merged.csv')
            .then(function(){
                fs.existsSync(__dirname + '/merged.csv').should.equal(true);
                done();
            }).catch(done);
        });
    });
    
});