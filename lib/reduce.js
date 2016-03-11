/*global require exports */

// map reduce.  this is the reduce part.  suppose there is a map of
// tasks, well there is.  now I am passed a single task, and ignoring
// all the others, I am going to process the task and report its
// results.

// except, apparently I am keeping the tally in the task.  Hmm.  That
// changes how the iteration happens, I guess


var _ = require('lodash')

// hard code some stuff, so I don't accidentally multiple lane miles
// by the hourly fraction which of course is nonsensical
var n_vars = ['sum_vmt']
var nhh_vars=['sum_single_unit_mt']
var hh_vars =['sum_combination_mt']

var header=["ts"
           ,"freeway"
           ,"n"
           ,"hh"
           ,"not_hh"
           ,"o"
           ,"avg_veh_spd"
           ,"avg_hh_weight"
           ,"avg_hh_axles"
           ,"avg_hh_spd"
           ,"avg_nh_weight"
           ,"avg_nh_axles"
           ,"avg_nh_spd"
           ,"miles"
           ,"lane_miles"
           ,"detector_count"
           ,"detectors"]
var unmapper = {}
for (var i=0,j=header.length; i<j; i++){
    unmapper[header[i]]=i
}

/**
 * handle_detector_data
 *
 */
function handle_detector_data(memo,item,cb){
    // add in the detector-based data to the task, mixing in with aadt_store
}

// fixme need a test on this for sure //

//  fixme probably should not be in this library //



/**
 * post_process_sql_queries
 *
 * pass in a task after it has hit postgres,
 *
 * returns a modified task, with duplicate roads from detectors
 * removed from the hpms sums, so that you don't double count
 *
 */
function post_process_sql_queries(task,cb){
    // this function merges the sql output from hpms and detectors

    // first, isolate the highways that are covered by detectors,
    // remove from hpms subtotals
    var detector_routes = _.uniq(_.map(task.detector_route_numbers, 'route_number')) || []
    var hpms_routes = _.uniq(_.map(task.accum, 'route_number')) || []
    var keep_hpms = _.difference(detector_routes,hpms_routes)
    var drop_hpms = _.difference(detector_routes,keep_hpms)

    // so only the "keep_hpms" highways get kept when summing up data

    var f_system_list = _.uniq(_.map(task.accum,'f_system'))
    var class_map={}
    var store = {}
    // filter out accum values with objectionable
    _.each(task.accum,function(row){
        var f = row.f_system

        if(_.indexOf(drop_hpms,row.route_number) === -1){
            // not on the skip list, so keep going
            // iterate over the types of sums for this functional class
            if(store[f] === undefined){
                store[f]={'sum_vmt':0
                         ,'sum_lane_miles':0
                         ,'sum_single_unit_mt':0
                         ,'sum_combination_mt':0
                         }
            }
            _.each(store[f],function(v,k){
                if(row[k]){
                    store[f][k]+=row[k]
                }
                return null
            })
        }else{
            // dropping this hpms data, but track the functional classification?
            if(row.route_number){
                if(class_map[row.route_number] === undefined){
                    class_map[row.route_number]=[f]
                }else{
                    class_map[row.route_number].push(f)
                }
            }
        }
    });

    // now for each functional class, I have AADT values.  Multiply
    // those by the grid's hourly fraction, and you get the hourly
    // AADT, etc.

    // I think that is a different function's job

    task.aadt_store = store
    task.class_map = class_map
    return cb(null,task)
}

function reduce_aadt_store(task,cb){
    // reduce the aadt_store, which is sums by functional class, so
    // that I get the summary of activity over all road types
    var store =  {'sum_vmt':0
                 ,'sum_lane_miles':0
                 ,'sum_single_unit_mt':0
                 ,'sum_combination_mt':0
                 }
    _.each(task.aadt_store,function(v,k){
        // k is the roadway functional class, v is the store
        _.each(v,function(vv,kk){
            store[kk]+=vv
            return null
        });
        return null
    });
    if(store.sum_vmt !== 0){
        task.aadt_totals=store
    }
    return cb(null,task)
}

exports.post_process_sql_queries=post_process_sql_queries
exports.reduce_aadt_store=reduce_aadt_store
