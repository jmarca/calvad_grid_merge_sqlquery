// if task.detector_fractions_hours === 0, then there is nothing in the current detector-based CalVAD with this data, and you can go without caring.

// if not, then you have to merge hpms and detector based data


var pg = require('pg')



var get_hpms_from_sql=function(task,cb){
    // if task has detector data, then have to be careful to ID routes
    // that match
    // sql

    var host = task.options.host ? task.options.host : '127.0.0.1';
    var user = task.options.username ? task.options.username : 'myname';
    var pass = task.options.password ? task.options.password : 'secret';
    var port = task.options.port ? task.options.port :  5432;
    var connectionString = "pg://"+user+":"+pass+"@"+host+":"+port+"/spatialvds";

    var grids_with_statement =
        ["hpmsgrids as",
         "(select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell",
         "     ,hd.hpms_id as hpms_id",
         "from carbgrid.state4k grids",
         "join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)",
         "join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)",
         "where floor(grids.i_cell) || '_'|| floor(grids.j_cell)='"+task.cell_id+"'",
         ")"].join(' ')

    var  data_with_statement =
        ["hpmsgeo as",
        "(select",
        "     id, year_record as year, state_code,is_metric,fips,begin_lrs,end_lrs,",
        "     route_number, type_facility,f_system,gf_system,section_length, aadt,through_lanes,",
        "     lane_width, peak_parking,",
        "     speed_limit, design_speed,",
        "     perc_single_unit,avg_single_unit,",
        "     perc_combination,avg_combination,",
        "     k_factor,dir_factor,",
        "     peak_lanes,peak_capacity,",
        "     county, locality,link_desc,from_name, to_name",
        "     ,hg.cell",
         "    ,CASE WHEN is_metric>0 THEN section_length*0.621371 ELSE section_length END as sec_len_miles",
        "from hpms.hpms_data hd",
        "join hpmsgrids hg on (hd.id=hg.hpms_id)",
        "where section_id !~ 'FHWA*' and state_code=6",
         "and year_record="+task.year,
         ")"].join(' ')
    var select_statement =
        ["select cell,year,route_number,f_system,"
        ,"sum(aadt) as sum_aadt, "
        ,"floor(sum(aadt*sec_len_miles)) as sum_vmt,"
        ,"sum(sec_len_miles*through_lanes) as sum_lane_miles "
        ,"from hpmsgeo "
        ,"group by cell,year,route_number,f_system "
        ,"order by cell,year,f_system"].join(' ')

    var query = 'with '
              + grids_with_statement
              + ','
              + data_with_statement+' '
              + select_statement
    pg.connect(connectionString
              ,function(err,client){
                   task.accum=[]
                   client.query(query, function(err, result) {
                       if(err) {
                           task.failed_query=query
                           return cb(err,task)
                       }
                       task.accum = result.rows
                       client.end()
                       return cb(null,task)
                   })
               })
    return null
}

exports.get_hpms_from_sql=get_hpms_from_sql
