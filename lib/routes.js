var queue = require('d3-queue').queue
var _=require('lodash')

var queries = require('./query_postgres')
var reduce = require('./reduce')
var f_system = require('./f_system')

var get_hpms_aadt = queries.get_hpms_from_sql
var process_sql = reduce.post_process_sql_queries
var get_detector_routes = queries.get_detector_route_nums
var ca = require('calvad_areas')
var grid_records= ca.grid_records
var ij_regex = /(\d*)_(\d*)/;
var pool_init = require('./pgpool.js')

/**
 *
 * @param {} req
 * @returns {}
 */
function task_init(req){
    return {'cell_id':req.params.i+'_'+req.params.j
           ,'cell_i':req.params.i
           ,'cell_j':req.params.j
           ,'year':req.params.yr}
}

/**
 *
 * @param {} task
 * @param {} cb
 * @returns {}
 */
function handler(task,pool,cb){
    queue()
        .defer(get_hpms_aadt, task, pool)
        .defer(get_detector_routes,task, pool)
    .await(function(error, t1, t2) {
        if(error) return cb(error)
        // the two tasks should be identical, and identical to task
        //console.log(t1, task);
        queue()
        .defer(process_sql,task)
        .await(function(error,t3){
            if(error) return cb(error)
            queue()
            .defer(reduce.reduce_aadt_store,task)
            .await(function(error,t){
                if(error) return cb(error)
                var result = []
                //console.log(t)
                _.forEach(t.aadt_store,mixer(task,result))
                result.sort(function(a,b){
                    return a.f_system - b.f_system
                })
                var totals = task.aadt_totals
                if(totals !== undefined){
                    totals.road_type ='totals'
                    totals.f_system  ='totals'
                    totals.cell_i    = task.cell_i
                    totals.cell_j    = task.cell_j
                    totals.year      = task.year
                    result.push(totals)
                }
                return cb(null,result)
            })
            return null
        });
        return null

    });
    return null
}



/**
 *
 * @param {} options
 * @param {} app
 * @returns {}
 */
function hpms_data_route (options,app){
    var pool = pool_init(options)
    app.get('/hpms/data/:yr/:i/:j.:format?'
           ,function(req,res,next){
                var task=task_init(req)
                task.options=options
               handler(task,pool,function(err,result){
                    if(err) return next(err)
                    res.json(result)
                    return null
                })
                return null
            })
    return null
}

/**
 *
 * @param {} task
 * @param {} cb
 * @returns {}
 * @private
 */
function nohwyhandler(task, pool ,cb){
                queue()
        .defer(get_hpms_aadt, task, pool)
                //.defer(get_detector_routes,task,pool)
                .await(function(error, t1, t2) {
                    if(error) return cb(error)
                    // the two tasks should be identical, and identical to task
                    //console.log(t1, task);
                    queue()
                    .defer(process_sql,task)
                    .await(function(error,t){
                        if(error) return cb(error)
                        queue()
                        .defer(reduce.reduce_aadt_store,task)
                        .await(function(error,t){
                            if(error) return cb(error)
                            var result = []
                            //console.log(t)
                            _.forEach(t.aadt_store,mixer(task,result))
                            result.sort(function(a,b){
                                return a.f_system - b.f_system
                            })
                            var totals = task.aadt_totals
                            if(totals!==undefined){
                                totals.road_type ='totals'
                                totals.f_system  ='totals'
                                totals.cell_i    = task.cell_i
                                totals.cell_j    = task.cell_j
                                totals.year      = task.year
                                result.push(totals)
                            }
                            return cb(null,result)
                        })
                        return null
                    })
                    return null
                });
    return null
}

/**
 *
 * @param {} options
 * @param {} app
 * @returns {}
 */
function hpms_data_nodetectors_route (options,app){
    var pool = pool_init(options)
    app.get('/hpms/dataonly/:yr/:i/:j.:format?'
           ,function(req,res,next){
                var task=task_init(req)
                task.options=options
                nohwyhandler(task,function(err,result){
                    if(err) return next(err)
                    res.json(result)
                    return null
                })
                return null
            })
    return null
}

/**
 *
 * @param {} task
 * @param {} result
 * @returns {}
 */
function mixer(task,result){
    return function(v,k){
        //console.log(Object.keys(v))
        //console.log(k)
        v.road_type=f_system[k] || k
        v.f_system=k
        v.cell_i = task.cell_i
        v.cell_j = task.cell_j
        v.year = task.year
        result.push(v)
    }
}
/**
 *
 * @param {} jobs
 * @param {} year
 * @param {} config
 * @param {} cb
 * @returns {}
 * @throws {}
 */
function all_hpms_handler (jobs,year,config,cb){
    var pool = pool_init(config)
    var tasks=
        _.map(grid_records,function(membership,cell_id){
            var re_result = ij_regex.exec(cell_id)
            return {'cell_id':cell_id
                    ,'cell_i':re_result[1]
                    ,'cell_j':re_result[2]
                    ,'year':year
                    ,'options':config
                   }
        });
    var q = queue(jobs);
    tasks.forEach(function(t) {
        q.defer(handler,t,pool);
    });
    q.awaitAll(function(error, results) {
        if(error){
            console.log("error: ",error)
            throw new Error(error)
        }
        // spit out results stitched together
        results = _.flatten(results)
        // results = _.filter(results,function(r){
        //           return r.f_system=='totals'
        //           })
        console.log("all done");
        cb(error,results)
        return null
    })
}


/**
 * all_hpms_route
 * @param {Object} config
 * @param {Numeric} config.jobs  default 4; how many simultaneous psql jobs, times 2
 * @param {Express.app} app
 * @returns {null}
 */
function all_hpms_route(config,app){
    var jobs = 4
    if (config.numjobs !== undefined && config.numjobs > 0){
        jobs = config.numjobs
    }
    app.get('/hpms/data/:yr.:format?'
           ,function(req,res,next){
               res.setTimeout(0)
               // build one task for each grid cell
               all_hpms_handler(jobs,req.params.yr,config,function(e,results){
                   if(e) {
                       throw new Error(e)
                   }
                   res.json(results)
                   return null
               })
               return null
            })
    return null
}




exports.hpms_data_route=hpms_data_route
exports.hpms_data_nodetectors_route=hpms_data_nodetectors_route
exports.hpms_data_handler=handler
exports.all_hpms_handler=all_hpms_handler
exports.all_hpms_route=all_hpms_route
