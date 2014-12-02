var queue = require("queue-async")
var _=require('lodash')

var queries = require('./query_postgres')
var reduce = require('./reduce')
var f_system = require('./f_system')

var get_hpms_aadt = queries.get_hpms_from_sql
var process_sql = reduce.post_process_sql_queries
var get_detector_routes = queries.get_detector_route_nums

function task_init(req){
    return {'cell_id':req.params.i+'_'+req.params.j
           ,'cell_i':req.params.i
           ,'cell_j':req.params.j
           ,'year':req.params.yr}
}
function hpms_data_route (options,app){
    app.get('/hpms/data/:yr/:i/:j.:format?'
           ,function(req,res,next){
                var task=task_init(req)
                task.options=options
                handler(task,function(err,result){
                    if(err) return next(err)
                    res.json(result)
                    return null
                })
                return null
            })
    return null
}

function hpms_data_nodetectors_route (options,app){
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

function mixer(task,result){
    return function(v,k){
        v.road_type=f_system[k]
        v.f_system=k
        v.cell_i = task.cell_i
        v.cell_j = task.cell_j
        v.year = task.year
        result.push(v)
    }
}
function handler(task,cb){
                queue()
                .defer(get_hpms_aadt, task)
                .defer(get_detector_routes,task)
                .await(function(error, t1, t2) {
                    if(error) return cb(error)
                    // the two tasks should be identical, and identical to task
                    //console.log(t1, task);
                    queue()
                    .defer(process_sql,task)
                    .await(function(error,t){
                        var result = []
                        if(error) return cb(error)
                        //console.log(t)
                        _.forEach(t.aadt_store,mixer(task,result))
                        result.sort(function(a,b){
                            return a.f_system - b.f_system
                        })
                        return cb(null,result)
                    })
                    return null
                });
}

function nohwyhandler(task,cb){
                queue()
                .defer(get_hpms_aadt, task)
                //.defer(get_detector_routes,task)
                .await(function(error, t1, t2) {
                    if(error) return cb(error)
                    // the two tasks should be identical, and identical to task
                    //console.log(t1, task);
                    queue()
                    .defer(process_sql,task)
                    .await(function(error,t){
                        var result = []
                        if(error) return cb(error)
                        //console.log(t)
                        _.forEach(t.aadt_store,mixer(task,result))
                        result.sort(function(a,b){
                            return a.f_system - b.f_system
                        })
                        return cb(null,result)
                    })
                    return null
                });

}
exports.hpms_data_route=hpms_data_route
exports.hpms_data_nodetectors_route=hpms_data_nodetectors_route
exports.hpms_data_handler=handler
