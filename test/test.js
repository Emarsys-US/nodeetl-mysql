/*jshint esversion: 6 */
/*jslint node: true */

const mocha = require('mocha'),
      chai = require('chai'),
      chaiAsPromised = require("chai-as-promised");
      MySQL = require('../index.js');
       
chai.use(chaiAsPromised);
chai.should(); 
  
const mysql = new MySQL({
  host     : 'localhost',
  user     : 'root',
  database : 'nodeetl-mysql'
});

describe('Class Methods', function(){
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
    });
    
    describe('Renames Tables', function(){
        it('Swaps Tables with Existing Table', function(){
            return mysql.swapTables('test').should.eventually.be.fulfilled;
        });
        
        it('Renames Table if not Existing Table is Present', function(done){
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