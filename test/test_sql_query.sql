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

-- Okay!  AADT is half, but vmt is identical.  So that is what I would
-- expect.  search for other bugs now

-- try with alameda county cells, multiple cells

-- 132_164

with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length((ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom)/st_length(hg.geom) as clipped_fraction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where floor(grids.i_cell) || '_'|| floor(grids.j_cell)='132_164'
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

  cell   | year | route_number | f_system | sum_aadt | sum_vmt |  sum_lane_miles   | sum_single_unit | sum_single_unit_mt | sum_combination | sum_combination_mt
---------+------+--------------+----------+----------+---------+-------------------+-----------------+--------------------+-----------------+--------------------
 132_164 | 2008 | 80           |       11 |  1287000 |  406005 |  16.0029255387443 |           16070 |               4041 |           26930 |               6483
 132_164 | 2008 | 80           |       12 |   162000 |  111280 |  5.49532300355264 |            3240 |               2225 |            1620 |               1112
 132_164 | 2008 |              |       14 |    28954 |    2831 | 0.391174455198743 |            1447 |                141 |             579 |                 56
(3 rows)



select * from hpms.hpms_link_geom  where hpms_id = 294641;


with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length((ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom)/st_length(hg.geom) as clipped_fraction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where hd.hpms_id = 294641
)
select * from hpmsgrids_all;


with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length((ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom)/st_length(hg.geom) as clipped_fraction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where floor(grids.i_cell) || '_'|| floor(grids.j_cell)='100_124'
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


-- all of alameda county

with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length((ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom)/st_length(hg.geom) as clipped_fraction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    join carb_counties_aligned_03 caco on st_intersects(grids.geom4326,caco.geom4326)
    where caco.name = 'ALAMEDA'
)
select * from hpmsgrids_all;


with hpmsgrids_all as (
    select distinct floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length((ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom)/st_length(hg.geom) as clipped_fraction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    join carb_counties_aligned_03 caco on st_intersects(grids.geom4326,caco.geom4326)
    where caco.name = 'ALAMEDA'
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

-- clipped_fractions > 1  in 133_164



with hpmsgrids_all as (
    select distinct floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length((ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom)/st_length(hg.geom) as clipped_fraction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where floor(grids.i_cell) || '_'|| floor(grids.j_cell)='133_164'
)
, hpmsgrids_summed as (
    select cell, hpms_id, sum(clipped_fraction) as clipped_fraction, count(clipped_fraction) as cnt from hpmsgrids_all group by cell, hpms_id
)
, hpmsgrids as (
    select cell,hpms_id, clipped_fraction
    from hpmsgrids_summed
    where clipped_fraction > 0.01
)
select * from hpmsgrids_summed order by clipped_fraction desc;


with hpmsgrids_all as (
    select distinct floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length((ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom)/st_length(hg.geom) as clipped_fraction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where floor(grids.i_cell) || '_'|| floor(grids.j_cell)='133_164'
)
, hpmsgrids_summed as (
    select cell, hpms_id, sum(clipped_fraction) as clipped_fraction, count(clipped_fraction) as cnt from hpmsgrids_all group by cell, hpms_id
)
, hpmsgrids as (
    select cell,hpms_id, clipped_fraction
    from hpmsgrids_summed
    where clipped_fraction > 0.01
)
select * from hpmsgrids_summed order by clipped_fraction desc;

-- greater than 1 clipped fraction.  why?  ( 266394,  220644, 323177,  266396,  220646,  323178)

with hpmsgrids_all as (
    select distinct floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length((ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom)/st_length(hg.geom) as clipped_fraction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where hpms_id in ( 266394,  220644, 323177,  266396,  220646,  323178)
)

with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length( (ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom) as clipped_length
      ,st_length((ST_DUMP(hg.geom)).geom) as orig_length
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where hpms_id in ( 266394,  220644, 323177,  266396,  220646,  323178)
)
select *,clipped_length/orig_length as frac  from hpmsgrids_all order by hpms_id;

with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,hg.id as geom_id
--      ,st_length( (ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom) as clipped_length
--      ,st_length((ST_DUMP(hg.geom)).geom) as orig_length
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where hpms_id in ( 266394,  220644, 323177,  266396,  220646,  323178)
)
select *  from hpmsgrids_all order by hpms_id;


--direction!

with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length( (ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom) as clipped_length
      ,st_length((ST_DUMP(hg.geom)).geom) as orig_length
      ,direction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where hpms_id in ( 266394,  220644, 323177,  266396,  220646,  323178)
)
, hpmsgrids_summed as (
    select cell, hpms_id, sum(clipped_length)/sum(orig_length) as clipped_fraction from hpmsgrids_all group by cell, hpms_id
)
select hpms_id,sum(clipped_fraction) from hpmsgrids_summed group by hpms_id;


-- need to separately sum lengths of roads


with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length( (ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom) as clipped_length
      ,hd.direction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where hpms_id in ( 266394,  220644, 323177,  266396,  220646,  323178,323773)
)
, hpmsgrids_summed as (
    select cell,hpms_id,sum(clipped_length) as clipped_length
    from hpmsgrids_all
    group by cell, hpms_id
)
, hpms_links as (
    select distinct hpms_id from hpmsgrids_summed
)
, hpms_only as (
    select hpms_id
           ,sum(st_length(hg.geom)) as orig_length
    from hpms_links hgs
    join hpms.hpms_link_geom hd using (hpms_id)
    join hpms.hpms_geom hg on (hd.geo_id = hg.id)
    group by hpms_id
)
-- , hpms_total_length as (
--     select hpms_id,sum(orig_length) as orig_length
--     from hpms_only
--     group by hpms_id
-- )
, hpms_fractional as (
    select cell,hpms_id,clipped_length,orig_length,clipped_length/orig_length as clipped_fraction
    from hpmsgrids_summed hgs
    join hpms_only htl using (hpms_id)
)
select hpms_id,sum(clipped_fraction) from hpms_fractional group by hpms_id;

-- sum up multiple cells for tests

with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length( (ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom) as clipped_length
      ,hd.direction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where floor(grids.i_cell) || '_'|| floor(grids.j_cell) in ( '100_223','189_72','132_164','134_164')
)
, hpmsgrids_summed as (
    select cell,hpms_id,sum(clipped_length) as clipped_length
    from hpmsgrids_all
    group by cell, hpms_id
)
, hpms_links as (
    select distinct hpms_id from hpmsgrids_summed
)
, hpms_only as (
    select hpms_id
           ,sum(st_length(hg.geom)) as orig_length
    from hpms_links hgs
    join hpms.hpms_link_geom hd using (hpms_id)
    join hpms.hpms_geom hg on (hd.geo_id = hg.id)
    group by hpms_id
)
, hpms_fractional as (
    select cell,hpms_id,clipped_length,orig_length,clipped_length/orig_length as clipped_fraction
    from hpmsgrids_summed hgs
    join hpms_only htl using (hpms_id)
)
, hpmsgrids as (
    select cell,hpms_id, clipped_fraction
    from hpms_fractional
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
, qury as (select cell,year,route_number,f_system, sum(aadt) as sum_aadt
       ,  floor(sum(aadt*sec_len_miles)) as sum_vmt
       , sum(sec_len_miles*through_lanes) as sum_lane_miles
       , floor(sum(avg_single_unit*aadt/100)) as sum_single_unit
       , floor(sum(avg_single_unit*aadt*sec_len_miles/100)) as sum_single_unit_mt
       , floor(sum(avg_combination*aadt/100)) as sum_combination
       , floor(sum(avg_combination*aadt*sec_len_miles/100)) as sum_combination_mt
from hpmsgeo
group by cell,year,route_number,f_system
order by cell,year,f_system
)
select cell,sum(sum_vmt) as total_vmt ,sum(sum_single_unit_mt) as total_smt ,sum(sum_combination_mt) as total_cmt from qury group by cell;

-- result
-- subtotals (qury above)
  cell   | year | route_number | f_system | sum_aadt | sum_vmt |  sum_lane_miles   | sum_single_unit | sum_single_unit_mt | sum_combination | sum_combination_mt
---------+------+--------------+----------+----------+---------+-------------------+-----------------+--------------------+-----------------+--------------------
 100_223 | 2008 |              |        8 |      470 |    1026 |  4.36698571248818 |               0 |                  0 |               0 |                  0
 132_164 | 2008 | 80           |       11 |  1287000 |  203688 |  8.02997442388714 |           16070 |               2016 |           26930 |               3233
 132_164 | 2008 | 80           |       12 |   162000 |   55166 |  2.72425098708635 |            3240 |               1103 |            1620 |                551
 132_164 | 2008 |              |       14 |    28954 |    2831 | 0.391174455198743 |            1447 |                141 |             579 |                 56
 134_164 | 2008 | 580          |        1 |   276000 |  203191 |  11.7791892187386 |            1380 |                985 |           12420 |               8871
 134_164 | 2008 | 580          |       11 |   304000 |  211246 |  12.1511943381134 |            1370 |               1936 |               0 |                  0
 134_164 | 2008 | 980          |       11 |   241000 |   76503 |  5.94206209730701 |            8150 |               2090 |            4520 |               1318
 134_164 | 2008 | 880          |       11 |   361000 |   75903 |  3.83924073906719 |           14830 |               3068 |           21220 |               4288
 134_164 | 2008 | 24           |       12 |   437000 |  128010 |  6.75881261885489 |            4090 |               1493 |            2800 |                902
 134_164 | 2008 | 61           |       14 |    20950 |    3579 | 0.341684499236609 |             838 |                143 |             209 |                 35
 134_164 | 2008 |              |       14 |   485985 |  290719 |  60.3411644142132 |             528 |                 53 |               0 |                  0
 134_164 | 2008 |              |       16 |   786381 |  387126 |  53.8207468521423 |            1793 |               1076 |            1170 |                735
 134_164 | 2008 |              |       17 |   202272 |   95764 |  32.8902318010478 |             492 |                 35 |             453 |                 33
 189_72  | 2008 | 101          |        2 |    29000 |   20119 |  2.77511038053919 |               0 |                  0 |               0 |                  0
 189_72  | 2008 | 101          |       12 |   267000 |   95177 |  4.29738998765844 |            6660 |               2569 |            7980 |               3236
 189_72  | 2008 | 225          |       14 |    23000 |    3921 | 0.340967300142049 |             460 |                 78 |               0 |                  0
 189_72  | 2008 | 192          |       14 |    13200 |   17172 |  2.60195419514455 |             396 |                515 |               0 |                  0
 189_72  | 2008 |              |       14 |   147893 |   93196 |  17.1478145302762 |            5180 |               3452 |            1381 |                950
 189_72  | 2008 |              |       16 |    92391 |   51948 |   11.371492329769 |            1341 |                357 |             546 |                147
 189_72  | 2008 | 192          |       16 |     2925 |    4252 |  2.90800165851872 |              58 |                 85 |               0 |                  0
 189_72  | 2008 |              |       17 |   153744 |   61567 |  39.8271147027099 |             742 |                254 |               0 |                  0


-- totals:
  cell   | total_vmt | total_smt | total_cmt
---------+-----------+------------+------------
 132_164 |    261685 |       3260 |       3840
 100_223 |      1026 |          0 |          0
 189_72  |    347352 |       7310 |       4333
 134_164 |   1472041 |      10879 |      16182



-- back to Alameda county

with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
      ,hd.hpms_id as hpms_id
      ,st_length( (ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom) as clipped_length
      ,hd.direction
    from carbgrid.state4k grids
    join carb_counties_aligned_03 caco on( st_intersects(grids.geom4326,caco.geom4326) and caco.name='ALAMEDA')
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
)
, hpmsgrids_summed as (
    select cell,hpms_id,sum(clipped_length) as clipped_length
    from hpmsgrids_all
    group by cell, hpms_id
)
, hpms_links as (
    select distinct hpms_id from hpmsgrids_summed
)
, hpms_only as (
    select hpms_id
           ,sum(st_length(hg.geom)) as orig_length
    from hpms_links hgs
    join hpms.hpms_link_geom hd using (hpms_id)
    join hpms.hpms_geom hg on (hd.geo_id = hg.id)
    group by hpms_id
)
-- , hpms_total_length as (
--     select hpms_id,sum(orig_length) as orig_length
--     from hpms_only
--     group by hpms_id
-- )
, hpms_fractional as (
    select cell,hpms_id,clipped_length,orig_length,clipped_length/orig_length as clipped_fraction
    from hpmsgrids_summed hgs
    join hpms_only htl using (hpms_id)
)
, hpmsgrids as (
    select cell,hpms_id, clipped_fraction
    from hpms_fractional
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
, qury as (select cell,year,route_number,f_system, sum(aadt) as sum_aadt
       ,  floor(sum(aadt*sec_len_miles)) as sum_vmt
       , sum(sec_len_miles*through_lanes) as sum_lane_miles
       , floor(sum(avg_single_unit*aadt/100)) as sum_single_unit
       , floor(sum(avg_single_unit*aadt*sec_len_miles/100)) as sum_single_unit_mt
       , floor(sum(avg_combination*aadt/100)) as sum_combination
       , floor(sum(avg_combination*aadt*sec_len_miles/100)) as sum_combination_mt
from hpmsgeo
group by cell,year,route_number,f_system
order by cell,year,f_system
)
select f_system,sum(sum_vmt) as total_vmt ,sum(sum_single_unit_mt) as total_smt ,sum(sum_combination_mt) as total_cmt from qury group by f_system;

-- result

 f_system | total_vmt | total_smt | total_cmt
----------+-----------+-----------+-----------
        1 |   2959661 |     65880 |    155097
        2 |    350860 |         0 |         0
        6 |    127998 |      2748 |      6552
        7 |    159653 |       298 |         0
        8 |     20174 |         0 |         0
       11 |  27464221 |    686326 |   1105982
       12 |   2183968 |     42321 |     22437
       14 |   7178007 |     73612 |     33739
       16 |   8095362 |     27860 |     11143
       17 |   3129546 |      6565 |      2024
       19 |    118060 |         0 |         0
totals:   |  51787510 |    905610 |   1336974

-- This is average daily vmt, of course, according to HPMS

-- now a simpler query to check that.. sum up all roads in Alameda

with hpmsgeo as (
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
           ,CASE WHEN is_metric>0
                 THEN section_length*0.621371
                 ELSE section_length
                 END as sec_len_miles
    from hpms.hpms_data hd
    where section_id !~ 'FHWA*'
    and state_code=6
    and year_record=2008
    and county='ALA'
)
, qury as (select year,route_number,f_system, sum(aadt) as sum_aadt
       ,  floor(sum(aadt*sec_len_miles)) as sum_vmt
       , sum(sec_len_miles*through_lanes) as sum_lane_miles
       , floor(sum(avg_single_unit*aadt/100)) as sum_single_unit
       , floor(sum(avg_single_unit*aadt*sec_len_miles/100)) as sum_single_unit_mt
       , floor(sum(avg_combination*aadt/100)) as sum_combination
       , floor(sum(avg_combination*aadt*sec_len_miles/100)) as sum_combination_mt
    from hpmsgeo
    group by year,route_number,f_system
    order by year,f_system
)
select f_system,sum(sum_vmt) as total_vmt ,sum(sum_single_unit_mt) as total_smt ,sum(sum_combination_mt) as total_cmt from qury group by f_system;

 f_system | total_vmt | total_smt | total_cmt
----------+-----------+-----------+-----------
        1 |   2460569 |     61125 |    143817
        2 |    410531 |       166 |       333
        6 |    112749 |      2138 |      3482
        7 |    247199 |       298 |         0
        8 |     29741 |         0 |         0
        9 |     27462 |         0 |         0
       11 |  17704769 |    450955 |    780441
       12 |   1578895 |     30602 |     16345
       14 |   5833486 |     61039 |     27622
       16 |   5819723 |     20473 |      8577
       17 |   2213288 |      4492 |      1197
       19 |   1967406 |         0 |         0
 total    |  38405818 |    631288 |    981814

totals:   |  51787510 |    905610 |   1336974

My way is too high, by almost twice
