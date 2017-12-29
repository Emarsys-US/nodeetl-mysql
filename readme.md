# Node ETL - MySQL Module

**What is this?**
This module / helper is used for interfacing with mysql. It helps with common tasks like loading files to tables, manipulating tables, etc.

This library is promise based. Most or all commands will return a promise.

## Getting Started

Npm install
```
$ npm install nodeetl-mysql
```

Require Library
```javascript
const MySQL = require('nodeetl-mysql');
```

Instantiate a mysql instance by passing the connection string
```javascript
let mysql = new MySQL('user:pass@mysqlurl:port/database');
```

(Optional) Enable debugging. You can pass winston to have messages log through your existing winston instance.
```javascript
const logger = requrie('winston');
// ... later
if(process.env.LOGGING_LEVEL === 'debug') mysql.setDebug(logger);
```

## Methods

**Utilities and Table Management**
* [getConnection](#getconnection)
* [query](#querycommand)
* [tableExists](#tableexiststable)
* [getHeaders](#getheadersfilepath-delimiter)
* [createStagingTable](#createstagingtabletable)
* [createNewTable](#createnewtablename-headers-overwrite)
* [swapTables](#swaptablestable)

**File Functions**
* [importFileToTable](#importfiletotableoptsobject)
* [importFileAndCreateTable](importfileandcreatetableoptsobject)

---

### getConnection()
When you create a new `mysql` instance, a pool is automatically generated. You can manually acquire a connection do run multiple sql commands if needed.

_Note_ This is primary used as an internal method. You usually won't need to use this.

**Example**
```javascript
mysql.getConnection()
.then(function(connection){
    connection.query('something');
})
```

**Returns** (Promise | Object)

A promise with the `connection` from the pool. You can release the connection back to the pool using `connection.release()`.

---

### query(command)
You can run a query through the `mysql` instance. This will automatically acquire a connection from the pool, run the command and release the connection for you. Any valid query can be used.

**Parameters**
* `command` (string) - The sql command to run

**Example**
```javascript
mysql.query(`CREATE TABLE copy LIKE original`)
.then(function(results){
    console.log(results);
}).catch(function(err){
    if(err) console.error(err);
})
```

**Returns** (Promise | Object)

A Promise with the `results` object from MySQL

---

### tableExists(table)
You can check if a table exists by passing it's name to this method.

**Parameters**
* `table` (sting) - name of table to look for

**Examples**
```javascript
mysql.tableExists('test')
.then(function(){
    // Table exists
})
.catch(function(){
    // Table does not exist
})

/*
 * You can also pass as fallback to first .then
 */
 mysql.tableExists('test')
 .then(
    function(){
     // Table exists
    }, 
    function(){
     // Table does not exist
    }
 )
```

**Returns** (Promise)

A promise - resolving if table is found, rejecting if table is not found.

---

### getHeaders(filepath, delimiter)
This is a utility for getting the headers of a text file

**Parameters**
* `filepath` (string) - Location of the file
* `delimiter` (optional | default "," | string) - Delimiter separating the fields. 

**Examples**
```javascript
mysql.getHeaders('./tmp/myfile.csv ')
.then(function(headers){
    // headers array
})

/**
 * Or you can pass a delimiter for non CSV files
 */
mysql.getHeaders('./tmp/myfile.tsv', '\t')
.then(function(headers){
    // headers array
})
```

**Returns** (Promise Array)

A Promise passing an `Array` of the headers from the file.

---

### createStagingTable('table')
This is mostly an internal method used by the class when importing files. But you can use it to create a staging copy of an existing table.

**Parameters**
* table (string) - The table to make a copy of

**Examples**
```javascript
mysql.createStagingTable('table')
.then(function(newTable){
    console.log(newTable);
    // 'table_staging'
})
```

**Returns** (Promise | String)

A promise with the name of the new table - always the name of the table passed + `_staging` like "table_staging"

---

### createNewTable(name, headers, overwrite)

**Parameters**
* `name` (string) - The name of the table to create
* `headers` (array) - An array of the header names. All headers are automatically set to VARCHAR(10000).
* `overwrite` (bool | Default `false`) - If an existing table already exists with the `name`, you can overwrite with by passing `true`.

**Examples**
```javascript
mysql.createNewTable('newTable', ['email', 'first', 'last'])
.then(function(newTable){
    // returns the name of the new table if successful
})
```

**Returns** (Promise | string)

Returns a promise containing the name of the new table created

---

### swapTables(table)
This is mostly an internal method used by the class for loading files, but can be invoked manually. This will find the matching production and staging version of the `table` paramater, drop the production version and rename the staging version.

This is used to reduce the amount of time a table or rows in a table are locked during data load. For example, this will be called after a file is loaded into `table_staging`. While the data is being loaded, `table` and all its rows will still be available for queries. Once the load is complete, `table` will be dropped and `table_staging` will be renamed to `table`.

In the event that `table` doesn't exist and only `table_staging` is found, then it simply renames `table_staging` to `table`.

**Parameters**
* `table` (string) - Name of table to swap/rename.

**Examples**
```javascript
mysql.swaptables('table')
.then(function(){
    // successfully swapped
})
.catch(function(err){
    // failed to swap / rename or couldn't find table_staging
})
```

**Returns** (Promise)

Resolves if the swap occurs successfully, rejects if there was an issue or table_staging wasn't found.

---

### importFileToTable(optsObject)
This method will import a file to an existing table. It's most useful when loading RDS data, where you know the structure of the table and the file will match one another and the table can be copied for import.

This function takes a table, makes a copy of it, loads all the file contents into it, and then swaps the copy with the live version of the table using `mysql.swapTables`.

This uses the Class method [createStagingTable](#createstagingtabletable) and MySQL's `LOAD DATA INFILE` command, [which is documented here](https://dev.mysql.com/doc/refman/5.7/en/load-data.html).

**Parameters** (object)
* `filepath` (string | required) - path to the file to load
* `table` (string | optional) - name of the table to load the file into. Fallsback to the file name if not provided.
* `headers` (array | optional) - If you only want to import certain fields from the file, you can pass the fields you want using this array.
* `delimiter` (string | optional | default = `","`) - The delimiter of the file you're importing. Defaults to comma.
* `quotes` (string | optional | default = `''`) - String enclosing each field of the file you're importing. Pass a single example of the character. For example, for quotes, pass `quotes: '"'`. Defaults to none.
* `newline` (string | optional | default = `"\n"`) - Line terminator in your file. Defaults to `\n`.

**Example**
```javascript
mysql.loadFileToTable({
    filepath: './tmp/example.csv',
    table: 'rds'
    headers: ['customerid', 'session date', 'wishlist id'],
    quotes: '"',
})
.then(function(rowsAffected){
    // rowsAffected = count of records imported
})
```

**Returns** (Promise | Int)

Returns a promise containing the number of rows affected. This is the number of records imported.

---

### importFileAndCreateTable(optsObject)
This method will create a new table and import a file into that table. It's most useful when you want to use a file to define a table, and quickly import it's data. This function is used by the merge method to import each file into a table before merging.

This function can create a new table with headers you pass or by reading the headers in the file. It can also overwrite an existing table if one exists, but you must pass the `overwrite` option as True.

If you're not going to use `overwrite` but plan to run this function on a recurring basis, make sure you drop the table after you're done with it so the next occurrence doesn't throw an error.

This uses the Class Method [createNewTable](#createnewtablename-headers-overwrite) and MySQL's `LOAD DATA INFILE` command, [which is documented here](https://dev.mysql.com/doc/refman/5.7/en/load-data.html).

**Parameters** (object)
* `filepath` (string | required) - path to the file to load
* `table` (string | optional) - name of the table to load the file into. Fallsback to the file name if not provided.
* `overwrite` (bool | default = `false`) - If an existing table with the `table` name exists, overwrite it. Defaults to false, throwing an error if the table already exists.
* `headers` (array | optional) - If you only want to import certain fields from the file, you can pass the fields you want using this array.
* `delimiter` (string | optional | default = `","`) - The delimiter of the file you're importing. Defaults to comma.
* `quotes` (string | optional | default = `''`) - String enclosing each field of the file you're importing. Pass a single example of the character. For example, for quotes, pass `quotes: '"'`. Defaults to none.
* `newline` (string | optional | default = `"\n"`) - Line terminator in your file. Defaults to `\n`.

**Example**
```javascript
mysql.importFileAndCreateTable({
    filepath: './tmp/example.csv',
    quotes: '"',
})
.then(function(rowsAffected){
    // rowsAffected = count of records imported
})

/**
 * Create a table with only 3 headers
 * Only import the matching headers from the file
 */
mysql.importFileAndCreateTable({
    filepath: './tmp/example.csv',
    headers: ['contactId', 'firstname', 'lastname']
    quotes: '"',
})
.then(function(rowsAffected){
    // rowsAffected = count of records imported
})
```

**Returns** (Promise | Int)

Returns a promise containing the number of rows affected. This is the number of records imported.

---

## Developing
To make changes to this class, ensure you document your methods or changes to any functionality. Additionally, make sure all unit testing passes. If you're adding a new method, write new tests in the /test/test.js file.

## Testing
You can run unit tests with:

```
$ npm test
```

## Changelog
v0.0.1 - Utility functions, tests, and readme