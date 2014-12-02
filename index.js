var q = require('./lib/query_postgres')
var r = require('./lib/reduce.js')
exports.get_hpms_from_sql=q.get_hpms_from_sql
exports.get_detector_route_nums = q.get_detector_route_nums
exports.pg_done=q.pg_done

exports.post_process_sql_queries=r.post_process_sql_queries
exports.apply_fractions=r.apply_fractions
exports.reduce=r.reduce
var f_system = require('./lib/f_system.json')
exports.f_system=f_system

var route = require('./lib/routes.js')

exports.hpms_data_route = route.hpms_data_route
