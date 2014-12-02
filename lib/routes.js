var queue = require("queue-async")

var queries = require('./query_postgres')
var reduce = require('./reduce')
var f_system = require('./f_system')

var get_hpms_aadt = queries.get_hpms_from_sql
var process_sql = reduce.post_process_sql_queries
var get_detector_routes = queries.get_detector_route_nums


function hpms_data_route (options,app){
    app.get('/hpms/data/:yr/:i/:j.:format?'
           ,function(req,res,next){
                var task={'cell_id':req.params.i+'_'+req.params.j
                         ,'year':req.params.yr
                         ,'options':options}

                queue()
                .defer(get_hpms_aadt, task)
                .defer(get_detector_routes,task)
                .await(function(error, t1, t2) {
                    if(error) return next(error)
                    // the two tasks should be identical, and identical to task
                    //console.log(t1, task);
                    queue()
                    .defer(process_sql,task)
                    .await(function(error,t){
                        var result = []
                        if(error) return next(error)
                        //console.log(t)
                        _.forEach(t.aadt_store,function(v,k){
                            v['road_type']=f_system[k]
                            v['f_system']=k
                            result.push(v)
                        })
                        result.sort(function(a,b){
                            return a.f_system - b.f_system
                        })
                        res.json(result)
                        return null
                    })
                    return null
                });
                return null
            })
    return null
}
exports.hpms_data_route=hpms_data_route
