// if task.detector_fractions_hours === 0, then there is nothing in the current detector-based CalVAD with this data, and you can go without caring.

// if not, then you have to merge hpms and detector based data

var cellmembership = require('calvad_areas').grid_records
var pg = require('pg')
var _ = require('lodash')


var get_hpms_from_sql=function(task,cb){
    // if task has detector data, then have to be careful to ID routes
    // that match
    // sql

    if(task.county === undefined ){
        task.county = cellmembership[task.cell_id].county
    }
    var host = task.options.postgres.host ? task.options.postgres.host : '127.0.0.1';
    var user = task.options.postgres.username ? task.options.postgres.username : 'myname';
    var pass = task.options.postgres.password ? task.options.postgres.password : 'secret';
    var port = task.options.postgres.port ? task.options.postgres.port :  5432;
    var db  = task.options.postgres.db ? task.options.postgres.db : 'spatialvds'
    var connectionString = "pg://"+user+":"+pass+"@"+host+":"+port+"/"+db

    var grid_with_statement =
        ["grid_cell as (",
         "select floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell",
         ",geom4326",
         "from carbgrid.state4k grids",
         "where (floor(grids.i_cell) || '_'|| floor(grids.j_cell)='100_223')",
         ")",
         ",fips_code as (",
         "select (regexp_matches(fips, '060*(\\d*)'))[1] as fips from counties_fips",
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
         "where section_id !~ 'FHWA*' and state_code=6",
         "and year_record="+task.year,
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
    console.log(query)
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

var get_detector_route_nums = function(task,cb){

    var host = task.options.postgres.host ? task.options.postgres.host : '127.0.0.1';
    var user = task.options.postgres.username ? task.options.postgres.username : 'myname';
    var pass = task.options.postgres.password ? task.options.postgres.password : 'secret';
    var port = task.options.postgres.port ? task.options.postgres.port :  5432;
    var db  = task.options.postgres.db ? task.options.postgres.db : 'spatialvds'
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
exports.get_detector_route_nums = get_detector_route_nums
exports.pg_done=function(cb){
    pg.end()
    if(cb){
        cb()
    }
    return null
}
