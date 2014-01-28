with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length((ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom)/st_length(hg.geom) as clipped_fraction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where floor(grids.i_cell) || '_'|| floor(grids.j_cell)='100_223'
)
, hpmsgrids as (
    select * from hpmsgrids_all
    where clipped_fraction > 0.01
)
,hpmsgeo as (
    select id, year_record as year, state_code,is_metric,fips,begin_lrs,end_lrs
           ,route_number, type_facility,f_system,gf_system, aadt,through_lanes
           ,lane_width, peak_parking
           ,speed_limit, design_speed
           , perc_single_unit as pct_s_u_pk_hr
           ,coalesce(avg_single_unit,0.0) as avg_single_unit
           ,perc_combination as pct_comb_pk_hr
           ,coalesce(avg_combination,0.0) as avg_combination
           ,k_factor,dir_factor
           ,peak_lanes,peak_capacity
           ,county, locality,link_desc,from_name, to_name
           ,hg.cell
           ,CASE WHEN is_metric>0
                 THEN section_length*clipped_fraction*0.621371
                 ELSE section_length*clipped_fraction
                 END as sec_len_miles
    from hpms.hpms_data hd
    join hpmsgrids hg on (hd.id=hg.hpms_id)
    where section_id !~ 'FHWA*'
    and state_code=6
    and year_record=2008
)
select cell,year,route_number,f_system, sum(aadt) as sum_aadt
       ,  floor(sum(aadt*sec_len_miles)) as sum_vmt
       , sum(sec_len_miles*through_lanes) as sum_lane_miles
       , floor(sum(avg_single_unit*aadt/100)) as sum_single_unit
       , floor(sum(avg_single_unit*aadt*sec_len_miles/100)) as sum_single_unit_mt
       , floor(sum(avg_combination*aadt/100)) as sum_combination
       , floor(sum(avg_combination*aadt*sec_len_miles/100)) as sum_combination_mt
from hpmsgeo
group by cell,year,route_number,f_system
order by cell,year,f_system


  cell   | year | route_number | f_system | sum_aadt | sum_vmt |  sum_lane_miles  | sum_single_unit | sum_single_unit_mt | sum_combination | sum_combination_mt
---------+------+--------------+----------+----------+---------+------------------+-----------------+--------------------+-----------------+--------------------
 100_223 | 2008 |              |        8 |      940 |    1026 | 4.36698571248818 |               0 |                  0 |               0 |                  0



-- working

with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length((ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom)/st_length(hg.geom) as clipped_fraction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where floor(grids.i_cell) || '_'|| floor(grids.j_cell)='100_223'
)
, hpmsgrids as (
    select * from hpmsgrids_all
    where clipped_fraction > 0.01
)
select * from hpmsgrids_all;
-- duplicates!

-- bug fixed with the summed bit, below

with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length((ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom)/st_length(hg.geom) as clipped_fraction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where floor(grids.i_cell) || '_'|| floor(grids.j_cell)='100_223'
)
, hpmsgrids_summed as (
    select cell, hpms_id, sum(clipped_fraction) as clipped_fraction from hpmsgrids_all group by cell, hpms_id
)
, hpmsgrids as (
    select cell,hpms_id, clipped_fraction
    from hpmsgrids_summed
    where clipped_fraction > 0.01
)
select * from hpmsgrids;

-- now combine with previous, check diffs
with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length((ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom)/st_length(hg.geom) as clipped_fraction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where floor(grids.i_cell) || '_'|| floor(grids.j_cell)='100_223'
)
, hpmsgrids_summed as (
    select cell, hpms_id, sum(clipped_fraction) as clipped_fraction from hpmsgrids_all group by cell, hpms_id
)
, hpmsgrids as (
    select cell,hpms_id, clipped_fraction
    from hpmsgrids_summed
    where clipped_fraction > 0.01
)
,hpmsgeo as (
    select id, year_record as year, state_code,is_metric,fips,begin_lrs,end_lrs
           ,route_number, type_facility,f_system,gf_system, aadt,through_lanes
           ,lane_width, peak_parking
           ,speed_limit, design_speed
           , perc_single_unit as pct_s_u_pk_hr
           ,coalesce(avg_single_unit,0.0) as avg_single_unit
           ,perc_combination as pct_comb_pk_hr
           ,coalesce(avg_combination,0.0) as avg_combination
           ,k_factor,dir_factor
           ,peak_lanes,peak_capacity
           ,county, locality,link_desc,from_name, to_name
           ,hg.cell
           ,CASE WHEN is_metric>0
                 THEN section_length*clipped_fraction*0.621371
                 ELSE section_length*clipped_fraction
                 END as sec_len_miles
    from hpms.hpms_data hd
    join hpmsgrids hg on (hd.id=hg.hpms_id)
    where section_id !~ 'FHWA*'
    and state_code=6
    and year_record=2008
)
select cell,year,route_number,f_system, sum(aadt) as sum_aadt
       ,  floor(sum(aadt*sec_len_miles)) as sum_vmt
       , sum(sec_len_miles*through_lanes) as sum_lane_miles
       , floor(sum(avg_single_unit*aadt/100)) as sum_single_unit
       , floor(sum(avg_single_unit*aadt*sec_len_miles/100)) as sum_single_unit_mt
       , floor(sum(avg_combination*aadt/100)) as sum_combination
       , floor(sum(avg_combination*aadt*sec_len_miles/100)) as sum_combination_mt
from hpmsgeo
group by cell,year,route_number,f_system
order by cell,year,f_system


  cell   | year | route_number | f_system | sum_aadt | sum_vmt |  sum_lane_miles  | sum_single_unit | sum_single_unit_mt | sum_combination | sum_combination_mt
---------+------+--------------+----------+----------+---------+------------------+-----------------+--------------------+-----------------+--------------------
 100_223 | 2008 |              |        8 |      940 |    1026 | 4.36698571248818 |               0 |                  0 |               0 |                  0


  cell   | year | route_number | f_system | sum_aadt | sum_vmt |  sum_lane_miles  | sum_single_unit | sum_single_unit_mt | sum_combination | sum_combination_mt
---------+------+--------------+----------+----------+---------+------------------+-----------------+--------------------+-----------------+--------------------
 100_223 | 2008 |              |        8 |      470 |    1026 | 4.36698571248818 |               0 |                  0 |               0 |                  0
(1 row)

-- Okay!  AADT is half, but vmt is identical.  So that is what I would expect.  search for other bugs now
