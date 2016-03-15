var cellmembership = require('calvad_areas').grid_records
var pg = require('pg')
var _ = require('lodash')


/**
 * the 2014 version of the query, that hits the newer database table
 * (hopefully 2014 and on, not just 2014...)
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
var get_hpms_from_sql_2014=function(task,cb){
    // if task has detector data, then have to be careful to ID routes
    // that match
    // sql

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

    var grid_with_statement =[
        "grid_cell as (",
        "  select floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell",
        "         ,geom4326",
        "  from carbgrid.state4k grids",
        "  where (floor(grids.i_cell) || '_'|| floor(grids.j_cell)='189_72')",
        "),",
        "fips_code as (",
        "  select (regexp_matches(fips, '060*(\d*)'))[1] as fips",
        "  from counties_fips",
        "  where upper(name)='SANTA BARBARA'",
        ")"
    ].join(' ')

    // this with statement gets details from the hpms data rewritten
    // from the original (pre-2014 data one) and has different results
    var  county_with_statement =[
        "hpms_grid_county as (",
        "  select grid_cell.cell,", //-- grid_cell.geom4326,",
        "         hd.gid as hpms_id, year_record as year,",
        "         state_code,fips,      route_id,      aadt,",
        "         section_length as full_sec_len_miles",
        "         ,st_transform(geom,4326) as geom",
        "         ,st_length( ST_Intersection(geom4326, st_transform(geom,4326))  ) as clipped_length",
        "         ,regexp_split_to_array(route_id,E'_') as name_parts",
        "  from grid_cell",
        "  join hpms.hpms_2014 hd  on ( st_intersects(geom4326,st_transform(geom,4326)) )",
        "  join fips_code fc on (1=1)",
        "  where ", //-- section_id !~ 'FHWA*' and
        "    year_record=2014",
        ")"
    ].join(' ')

    var joined_with_statement = [
        "hpmsgrids_summed as (",
        "  select cell, hpms_id,",
        "         sum(clipped_length) as clipped_length",
        "  from hpms_grid_county",
        "  group by cell, hpms_id",
        "),",
        "hpms_links as (",
        "  select distinct hpms_id",
        "  from hpmsgrids_summed",
        "),",
        "hpms_only as (",
        "  select hpms_id",
        "         ,st_length(hg.geom) as orig_length",
        "  from hpms_links hgs",
        "  join hpms.hpms_2014 hg on (hg.gid = hgs.hpms_id)",
        "),",
        "hpms_fractional as (",
        "  select cell,hpms_id,clipped_length,orig_length,clipped_length/orig_length as clipped_fraction",
        "  from hpmsgrids_summed hgs",
        "  join hpms_only htl using (hpms_id)",
        "),",
        "hpms_fractional_limited as (",
        "  select cell,hpms_id, clipped_fraction",
        "  from hpms_fractional",
        "  where clipped_fraction > 0.01",
        "),",
        "names_cities as (",
        "  select hgc.*,",
        "  case",
        "    when array_length(name_parts,1) = 4 then upper(name_parts[2])",
        "    when name_parts[1] ~* 'SHS' then 'SHS'",
        "    else name_parts[1]",
        "  end as city_abbrev",
        "  from hpms_grid_county hgc",
        "),",
        "city_join as (",
        "  select hgc.*,coalesce(ca.name,hgc.city_abbrev) as city_name",
        "  from names_cities hgc",
        "  left outer join geocoding.city_abbrev ca on (upper(ca.abbrev)=hgc.city_abbrev)",
        "),",
        "hpms_joined as (",
        "  select hga.*, clipped_fraction, clipped_fraction*full_sec_len_miles as sec_len_miles",
        ",case",
        "  when array_length(name_parts,1) = 4 then city_name || ' ' || name_parts[4]",
        "  when name_parts[1] ~* 'SHS' then route_id",
        "  else name_parts[1]",
        "end as classify",
        ",case",
        "  when name_parts[1] ~* 'SHS' then name_parts[2]",
        "end as route_number",
        "  from city_join hga",
        "  join hpms_fractional_limited hfl on (hga.hpms_id=hfl.hpms_id)",
        ")"
    ].join(' ')

    var select_statement =[
        "select cell,year,classify,route_number,",
        //,"       -- f_system,",
        //,"       -- hpms_id,route_id,",
        ,"       sum(aadt) as sum_aadt,",
        ,"       floor(sum(aadt*sec_len_miles)) as sum_vmt",
        //,"--       ,sum(sec_len_miles*through_lanes) as sum_lane_miles",
        //,"--       ,floor(sum(avg_single_unit*aadt/100)) as sum_single_unit",
        //,"--       ,floor(sum(avg_single_unit*aadt*sec_len_miles/100)) as sum_single_unit_mt",
        //,"--       ,floor(sum(avg_combination*aadt/100)) as sum_combination",
        //,"--       ,floor(sum(avg_combination*aadt*sec_len_miles/100)) as sum_combination_mt",
        ,"from hpms_joined",
        ,"group by cell,year,classify,route_number",
        ,"order by cell,year,classify,route_number"
    ].join(' ')

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
                               if(k!=='cell' && k !== 'classify')  row[k]=+v
                           })
                       });
                       clientdone()

                       return cb(null,task)
                   })
                   return null
               })
    return null
}
exports.get_hpms_from_sql_2014=get_hpms_from_sql_2014
