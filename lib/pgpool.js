var Pool = require('pg').Pool;

var pool
function pool_init(options){
    if (pool !== undefined){
        return pool
    }
    // initialize the postgresql pool
    var host = options.postgresql.host ? options.postgresql.host : '127.0.0.1';
    var user = options.postgresql.username ? options.postgresql.username : 'myname';
    var pass = options.postgresql.password
    var port = options.postgresql.port ? options.postgresql.port :  5432;
    var db  = options.postgresql.grid_merge_sqlquery_db ? options.postgresql.grid_merge_sqlquery_db : 'spatialvds'

    var connectionParams = {
        'user': user,
        'host': host,
        'database': db,
        'max': 10, // max number of clients in pool
        'idleTimeoutMillis': 1000 // close & remove clients which have been idle > 1 second
    }
    if(pass !== undefined){
        connectionParams.password = pass
    }

    pool = new Pool(connectionParams)
    pool.on('error', function(e, client) {
        // if a client is idle in the pool
        // and receives an error - for example when your PostgreSQL server restarts
        // the pool will catch the error & let you handle it here
        //
        // not sure when this might arise, so for now, just die
        throw new Error(e)
    });

    return pool
}
module.exports=pool_init
