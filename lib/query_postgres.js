// if task.detector_fractions_hours === 0, then there is nothing in the current detector-based CalVAD with this data, and you can go without caring.

// if not, then you have to merge hpms and detector based data

var cellmembership = require('calvad_areas').grid_records
var pg = require('pg')
var _ = require('lodash')
var other = require('./query_postgres_2014.js')
// fixme need to figure out jsdoc for required vs optional parameters
// fixme link to the hpms2006.pdf workbook for definition of variables
//
/**
 * query the hpms data table, and joined geometries.  this is for the version
 * prior to the 2014 HPMS data, with geocoding done by hand.
 *
 * The results of the query will be stored in the passed-in task
 * object.  If there is an issue with the query, then the failed query
 * will be stored in task.failed_query, and the callback will be
 * called with a non-null error value, as well as the task object as
 * the second argument.
 *
 * If the query succeeds, then the rows returned by the query will be
 * copied to task.accum, which of course stands for accumulate but is
 * easier to type.  I also run a little bit of code to make sure that
 * each value in each result row is numeric, because they should be,
 * except for the"cell" entry, which is a string.
 *
 * So, the basic idea is to pass in the cell you are interested in as
 * the parameter task.cell_id, and the year as the parameter
 * task.year, and this function will run off to the postgresql
 * database (defined by task.options.postgresql) and run the query.
 *
 * It will return
 *
 * the cell
 * the year
 * the route_number
 * the f_system value (the roadway functional system)
 * sum(aadt) as sum_aadt
 * floor(sum(aadt*sec_len_miles)) as sum_vmt
 * sum(sec_len_miles*through_lanes) as sum_lane_miles
 * floor(sum(avg_single_unit*aadt/100)) as sum_single_unit
 * floor(sum(avg_single_unit*aadt*sec_len_miles/100)) as sum_single_unit_mt
 * floor(sum(avg_combination*aadt/100)) as sum_combination
 * floor(sum(avg_combination*aadt*sec_len_miles/100)) as sum_combination_mt
 *
 * The formal definitions of these variables can be found in the HPMS
 * guide.  Briefly, the functional system is defined as follows:
 *
 {"1" : "Rural Principal Arterial Interstate (PAI)",
 "2" : "Rural Other Principal Arterial (OPA)",
 "6" : "Rural Minor Arterial (MA)",
 "7" : "Rural Major Collector (MJC)",
 "8" : "Rural Minor Collector (MNC)",
 "9" : "Rural Local (LOC)",
 "11" : "Urban Principal Arterial Interstate (PAI)",
 "12" : "Urban Principal Arterial Other Fwys & Exp (OFE)",
 "14" : "Urban Other Principal Arterial (OPA)",
 "16" : "Urban Minor Arterial (MA)",
 "17" : "Urban Collector (COL)",
 "19" : "Urban Local (LOC)",
 *
 * avg_single_unit is the average percentage of single unit trucks in
 * the daily flow, which is why it is divided by 100. Similarly, the
 * avg_combination is the average daily percentage of vehicles that
 * are combination trucks.
 *
 * sec_len_miles is the length of the section, truncated by the grid
 * in question, in miles.  Sometimes the HPMS data has kilometers not
 * miles, and the correct conversion is performed.
 *
 * @param {!Object} task the current task with all the details
 * @param {!number} task.year the year.  if > 2009, will switch to 2014 table version
 * @param {!string} task.cell_id the cell id, in the form 'i_j'
 * @param {!Object} task.options the config information for postgresql, etc
 * @param {!Object} task.options.postgresql the config information for postgresql
 * @param {!string} task.options.postgresql.host the database host, for example 'localhost' or '127.0.0.1' or 'example.com'.  Certainly don't need 'http://' or anything. If in doubt, see the documentation for the node-postgres module
 * @param {!string} task.options.postgresql.username  the pg username.
 * @param {string}  task.options.postgresql.password  the pg password.  Optional if you don't use passwords for the db in question, or if you have a valid .pgpass file (see the PostgreSQL website for documentation of .pgpass file).
 * @param {number} task.options.posgresql.port the port for the database, defaults to 5432
 * @param {!string} task.options.posgresql.grid_merge_sqlquery_db the databse to use for the queries.  Notice how I cleverly named this option after this package?  Except slightly different?  Oh my!  Required, or it will default to 'spatialvds'
 * @param {} cb the callback to send the results of the query
 * @returns {} null
 */
var get_hpms_from_sql=function(task,cb){
    // if task has detector data, then have to be careful to ID routes
    // that match
    // sql

    // if year > 2009, switch to that one
    if(task.year > 2009) return other.get_hpms_from_sql_2014(task,cb)

    if(task.county === undefined ){
        task.county = cellmembership[task.cell_id].county
    }
    var host = task.options.postgresql.host ? task.options.postgresql.host : '127.0.0.1';
    var user = task.options.postgresql.username ? task.options.postgresql.username : 'myname';
    var pass = task.options.postgresql.password ? task.options.postgresql.password : '';
    var port = task.options.postgresql.port ? task.options.postgresql.port :  5432;
    var db  = task.options.postgresql.grid_merge_sqlquery_db ? task.options.postgresql.grid_merge_sqlquery_db : 'spatialvds'
    var connectionString = "pg://"+user+":"+pass+"@"+host+":"+port+"/"+db

    // this with statement is actually two with statments.  First is
    // the grid_cell, which gets the requested grid cell and its
    // geometry.  The second with statement, fips_code, gets the fips
    // code from counties_fips, based on the county name.  If you
    // don't ask for a county, then we have issues below.

    var grid_with_statement =
        ["grid_cell as (",
         "select floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell",
         ",geom4326",
         "from carbgrid.state4k grids",
         "where (floor(grids.i_cell) || '_'|| floor(grids.j_cell)='"+task.cell_id+"')",
         ")",
         ",fips_code as (",
         "select (regexp_matches(fips, '060*(\\\d*)'))[1] as fips from counties_fips",
         "where upper(name)='"+task.county+"'",
         ")"
        ].join(' ')

    var  county_with_statement =
        ["hpms_grid_county as",
        "(select grid_cell.cell,grid_cell.geom4326,",
        "     hd.id as hpms_id, year_record as year, state_code,is_metric,hd.fips,begin_lrs,end_lrs,",
        "     route_number, type_facility,f_system,gf_system, aadt,through_lanes,",
        "     lane_width, peak_parking,",
        "     speed_limit, design_speed,",
        "     perc_single_unit as pct_s_u_pk_hr,",
        "     coalesce(avg_single_unit,0.0) as avg_single_unit,",
        "     perc_combination as pct_comb_pk_hr,",
        "     coalesce(avg_combination,0.0) as avg_combination,",
        "     k_factor,dir_factor,",
        "     peak_lanes,peak_capacity,",
        "     county, locality,link_desc,from_name, to_name",
         "    ,CASE WHEN is_metric>0 THEN section_length*0.621371 ELSE section_length END as full_sec_len_miles",
         ",hg.*",
         ",st_length( ST_Intersection(geom4326, hg.geom)  ) as clipped_length",
         "from hpms.hpms_data hd",
         "     join hpms.hpms_link_geom hlg on (hlg.hpms_id=hd.id)",
         "     join hpms.hpms_geom hg on (hg.id=hlg.geo_id)",
         "     inner join grid_cell on ( st_intersects(geom4326,geom) )",
         "     join fips_code fc on (fc.fips = hd.fips)",
         "where",
         // actually, at this time none of the FHWA records are
         // geocoded, so this next limiter does nothing
         "section_id !~ 'FHWA*' and ",
         "state_code=6 and",
         "year_record="+task.year,
         ")"].join(' ')

    var joined_with_statement =
        ["hpmsgrids_summed as (",
         "  select cell, hpms_id, sum(clipped_length) as clipped_length",
         "  from hpms_grid_county group by cell, hpms_id",
         ")",
         ", hpms_links as (",
         "    select distinct hpms_id from hpmsgrids_summed",
         ")",
         ", hpms_only as (",
         "    select hpms_id",
         "           ,sum(st_length(hg.geom)) as orig_length",
         "    from hpms_links hgs",
         "    join hpms.hpms_link_geom hd using (hpms_id)",
         "    join hpms.hpms_geom hg on (hd.geo_id = hg.id)",
         "    group by hpms_id",
         ")",
         ", hpms_fractional as (",
         "    select cell,hpms_id,clipped_length,orig_length,clipped_length/orig_length as clipped_fraction",
         "    from hpmsgrids_summed hgs",
         "    join hpms_only htl using (hpms_id)",
         ")",
         ", hpms_fractional_limited as (",
         "  select cell,hpms_id, clipped_fraction",
         "  from hpms_fractional",
         "  where clipped_fraction > 0.01",
         ")",
         ", hpms_joined as (",
         "  select hga.*, clipped_fraction, clipped_fraction*full_sec_len_miles as sec_len_miles",
         "  from hpms_grid_county hga",
         "  join hpms_fractional_limited hfl on (hga.hpms_id=hfl.hpms_id)",
         ")"
        ].join(' ')

    var select_statement =
        ["select cell,year,route_number,f_system,"
        ,"sum(aadt) as sum_aadt, "
        ,"floor(sum(aadt*sec_len_miles)) as sum_vmt,"
        ,"sum(sec_len_miles*through_lanes) as sum_lane_miles,"
        ,"floor(sum(avg_single_unit*aadt/100)) as sum_single_unit,"
        ,"floor(sum(avg_single_unit*aadt*sec_len_miles/100)) as sum_single_unit_mt,"
        ,"floor(sum(avg_combination*aadt/100)) as sum_combination,                "
        ,"floor(sum(avg_combination*aadt*sec_len_miles/100)) as sum_combination_mt"
        ,"from hpms_joined "
        ,"group by cell,year,route_number,f_system "
        ,"order by cell,year,f_system"].join(' ')

    var query = 'with '
              + grid_with_statement
              + ', '
              + county_with_statement
              + ','
              + joined_with_statement
              + ' '
              + select_statement
    //console.log(query)
    pg.connect(connectionString
              ,function(err,client,clientdone){
                   if(err){
                       console.error('error fetching client from pool', err);
                       return cb(err)
                   }
                   task.accum=[]
                   client.query(query, function(err, result) {
                       if(err) {
                           // console.log('query failed '+query)
                           task.failed_query=query
                           clientdone()
                           return cb(err,task)
                       }
                       //console.log(_.size(result.rows) + ' records returned')
                       task.accum = result.rows
                       // make sure values are numeric
                       _.each(task.accum,function(row){
                           _.each(row,function(v,k){
                               if(k!=='cell')  row[k]=+v
                           })
                       });
                       clientdone()

                       return cb(null,task)
                   })
                   return null
               })
    return null
}

// this has a problem
//
// the issue is that I am looking for detectors based on spatial
// overlap, disregarding time, and not based on having actual data.
// so I will likely end up with route numbers in the grid cell that
// sometimes do not have real data from detectors
//
// what I need to do is to grab from the couchdb data for the period
// in question, for the grid in question, and if there is data check
// what detectors, then get from the db what the detectors' freeway
// numbers are.

// for example, check out http://127.0.0.1:5984/_utils/document.html?carb%2Fgrid%2Fstate4k/134_164_2007-01-01%2001%3A00

// the doc has:

// {
//    "_id": "134_164_2007-01-01 01:00",
//    "_rev": "1-4bc122d9d6090e62c32131cd4670f084",
//    "geom_id": "134_164",
//    "i_cell": 134,
//    "j_cell": 164,
//    "data": [
//        "2007-01-01 01:00",
//        "980",
//        583.22,
//        55.9,
//        64.29,
//        0.02,
//        79.18,
//        46.91,
//        5.07,
//        65.42,
//        16.56,
//        2.21,
//        68.14,
//        1.32,
//        4.25,
//        3,
//        "400642",
//        "401137",
//        "401413"
//    ],
//    "aadt_frac": {
//        "n": 0.0007436103062443218,
//        "hh": 0.0023488514735826088,
//        "not_hh": 0.002544687951105734
//    }
// }

// those 400643 type numbers are the detectors that are in the grid cell.



var get_detector_route_nums = function(task,cb){

    var host = task.options.postgresql.host ? task.options.postgresql.host : '127.0.0.1';
    var user = task.options.postgresql.username ? task.options.postgresql.username : 'myname';
    var pass = task.options.postgresql.password ? task.options.postgresql.password : '';
    var port = task.options.postgresql.port ? task.options.postgresql.port :  5432;
    var db  = task.options.postgresql.grid_merge_sqlquery_db ? task.options.postgresql.grid_merge_sqlquery_db : 'die'
    var connectionString = "pg://"+user+":"+pass+"@"+host+":"+port+"/"+db
    var query =
        ["select distinct floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell"
        ,",refnum as route_number"
        ,"from carbgrid.state4k grids"
        ,"join tempseg.tdetector ttd on st_intersects(ttd.geom,grids.geom4326)"
        ,"where floor(grids.i_cell) || '_'|| floor(grids.j_cell)='"+task.cell_id+"'"
        ].join(' ')
    //console.log(query)
    pg.connect(connectionString
              ,function(err,client,clientdone){
                   if(err){
                       console.error('error fetching client from pool', err);
                       return cb(err)
                   }
                   task.accum=[]
                   client.query(query, function(err, result) {
                       if(err) {
                           task.failed_query=query
                           clientdone()
                           return cb(err)
                       }
                       task.detector_route_numbers =
                           _.map(result.rows,function(r){
                               return {'cell':r.cell,'route_number':+r.route_number}
                           });
                       clientdone()
                       return cb(null,task)
                   })
                   return null
               })
    return null
}

exports.get_hpms_from_sql=get_hpms_from_sql
exports.get_hpms_from_sql_2014=other.get_hpms_from_sql_2014
exports.get_detector_route_nums = get_detector_route_nums
exports.pg_done=function(cb){
    pg.end()
    if(cb){
        cb()
    }
    return null
}
