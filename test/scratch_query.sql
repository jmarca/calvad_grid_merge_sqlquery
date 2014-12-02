-- this is the current query, for a single grid 189_72
with grid_cell as (
     select floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
     ,geom4326
     from carbgrid.state4k grids
     where (floor(grids.i_cell) || '_'|| floor(grids.j_cell)='189_72')
     )
,fips_code as (
     select (regexp_matches(fips, '060*(\d*)'))[1] as fips
     from counties_fips
     where upper(name)='SANTA BARBARA'
     )
, hpms_grid_county as (
     select grid_cell.cell,grid_cell.geom4326,
     hd.id as hpms_id, year_record as year,
     state_code,is_metric,hd.fips,begin_lrs,end_lrs,
     route_number, type_facility,f_system,gf_system, aadt,through_lanes,
     lane_width, peak_parking,
     speed_limit, design_speed,
     perc_single_unit as pct_s_u_pk_hr,
     coalesce(avg_single_unit,0.0) as avg_single_unit,
     perc_combination as pct_comb_pk_hr,
     coalesce(avg_combination,0.0) as avg_combination,
     k_factor,dir_factor,
     peak_lanes,peak_capacity,
     county, locality,link_desc,from_name, to_name
     ,CASE WHEN is_metric>0 THEN section_length*0.621371 ELSE section_length END as full_sec_len_miles
     ,hg.* ,st_length( ST_Intersection(geom4326, hg.geom)  ) as clipped_length
     from hpms.hpms_data hd
     join hpms.hpms_link_geom hlg on (hlg.hpms_id=hd.id)
     join hpms.hpms_geom hg on (hg.id=hlg.geo_id)
     inner join grid_cell on ( st_intersects(geom4326,geom) )
     join fips_code fc on (fc.fips = hd.fips)
     where
     section_id !~ 'FHWA*' and
     state_code=6 and
     year_record=2009
     )
,hpmsgrids_summed as (
     select cell, hpms_id, sum(clipped_length) as clipped_length
     from hpms_grid_county group by cell, hpms_id
     )
,hpms_links as (
     select distinct hpms_id from hpmsgrids_summed
     )
,hpms_only as (
     select hpms_id
     ,sum(st_length(hg.geom)) as orig_length
     from hpms_links hgs
     join hpms.hpms_link_geom hd using (hpms_id)
     join hpms.hpms_geom hg on (hd.geo_id = hg.id)
     group by hpms_id
     )
,hpms_fractional as (
     select cell,hpms_id,clipped_length,orig_length,clipped_length/orig_length as clipped_fraction
     from hpmsgrids_summed hgs
     join hpms_only htl using (hpms_id)
     )
,hpms_fractional_limited as (
     select cell,hpms_id, clipped_fraction
     from hpms_fractional
     where clipped_fraction > 0.01
     )
,hpms_joined as (
     select hga.*, clipped_fraction, clipped_fraction*full_sec_len_miles as sec_len_miles
     from hpms_grid_county hga
     join hpms_fractional_limited hfl on (hga.hpms_id=hfl.hpms_id)
     )
select
     cell,year,route_number,f_system, sum(aadt) as sum_aadt,
     floor(sum(aadt*sec_len_miles)) as sum_vmt,
     sum(sec_len_miles*through_lanes) as sum_lane_miles,
     floor(sum(avg_single_unit*aadt/100)) as sum_single_unit,
     floor(sum(avg_single_unit*aadt*sec_len_miles/100)) as sum_single_unit_mt,
     floor(sum(avg_combination*aadt/100)) as sum_combination,
     floor(sum(avg_combination*aadt*sec_len_miles/100)) as sum_combination_mt
     from hpms_joined
     group by cell,year,route_number,f_system
     order by cell,year,f_system;

--- result:
--   cell  | year | route_number | f_system | sum_aadt | sum_vmt |  sum_lane_miles   | sum_single_unit | sum_single_unit_mt | sum_combination | sum_combination_mt
-- --------+------+--------------+----------+----------+---------+-------------------+-----------------+--------------------+-----------------+--------------------
--  189_72 | 2009 | 101          |        2 |    29000 |   20119 |  2.77508869053543 |               0 |                  0 |               0 |                  0
--  189_72 | 2009 | 101          |       12 |   267000 |   95487 |  4.29814414732388 |            6670 |               2581 |            8000 |               3252
--  189_72 | 2009 |              |       14 |   151196 |   93787 |   17.150024415496 |            5098 |               3383 |            1381 |                950
--  189_72 | 2009 | 192          |       14 |    12300 |   16001 |  2.60181767868063 |             369 |                480 |               0 |                  0
--  189_72 | 2009 | 225          |       14 |    22000 |    3747 | 0.340672814821636 |               0 |                  0 |               0 |                  0
--  189_72 | 2009 | 192          |       16 |     3100 |    4507 |  2.90801266914361 |              62 |                 90 |               0 |                  0
--  189_72 | 2009 |              |       16 |    92391 |   51935 |  11.3696098361967 |            1034 |                306 |             240 |                 97
--  189_72 | 2009 |              |       17 |   153724 |   61562 |   39.826748623163 |             547 |                130 |               0 |                  0
-- (8 rows)


-- now without the skipping of the fhwa nodes?  Are those even geocoded?

with grid_cell as (
     select floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
     ,geom4326
     from carbgrid.state4k grids
     where (floor(grids.i_cell) || '_'|| floor(grids.j_cell)='189_72')
     )
,fips_code as (
     select (regexp_matches(fips, '060*(\d*)'))[1] as fips
     from counties_fips
     where upper(name)='SANTA BARBARA'
     )
, hpms_grid_county as (
     select grid_cell.cell,grid_cell.geom4326,
     hd.id as hpms_id, year_record as year,
     state_code,is_metric,hd.fips,begin_lrs,end_lrs,
     route_number, type_facility,f_system,gf_system, aadt,through_lanes,
     lane_width, peak_parking,
     speed_limit, design_speed,
     perc_single_unit as pct_s_u_pk_hr,
     coalesce(avg_single_unit,0.0) as avg_single_unit,
     perc_combination as pct_comb_pk_hr,
     coalesce(avg_combination,0.0) as avg_combination,
     k_factor,dir_factor,
     peak_lanes,peak_capacity,
     county, locality,link_desc,from_name, to_name
     ,CASE WHEN is_metric>0 THEN section_length*0.621371 ELSE section_length END as full_sec_len_miles
     ,hg.* ,st_length( ST_Intersection(geom4326, hg.geom)  ) as clipped_length
     from hpms.hpms_data hd
     join hpms.hpms_link_geom hlg on (hlg.hpms_id=hd.id)
     join hpms.hpms_geom hg on (hg.id=hlg.geo_id)
     inner join grid_cell on ( st_intersects(geom4326,geom) )
     join fips_code fc on (fc.fips = hd.fips)
     where
     --     section_id !~ 'FHWA*' and
     state_code=6 and
     year_record=2009
     )
,hpmsgrids_summed as (
     select cell, hpms_id, sum(clipped_length) as clipped_length
     from hpms_grid_county group by cell, hpms_id
     )
,hpms_links as (
     select distinct hpms_id from hpmsgrids_summed
     )
,hpms_only as (
     select hpms_id
     ,sum(st_length(hg.geom)) as orig_length
     from hpms_links hgs
     join hpms.hpms_link_geom hd using (hpms_id)
     join hpms.hpms_geom hg on (hd.geo_id = hg.id)
     group by hpms_id
     )
,hpms_fractional as (
     select cell,hpms_id,clipped_length,orig_length,clipped_length/orig_length as clipped_fraction
     from hpmsgrids_summed hgs
     join hpms_only htl using (hpms_id)
     )
,hpms_fractional_limited as (
     select cell,hpms_id, clipped_fraction
     from hpms_fractional
     where clipped_fraction > 0.01
     )
,hpms_joined as (
     select hga.*, clipped_fraction, clipped_fraction*full_sec_len_miles as sec_len_miles
     from hpms_grid_county hga
     join hpms_fractional_limited hfl on (hga.hpms_id=hfl.hpms_id)
     )
select
     cell,year,route_number,f_system, sum(aadt) as sum_aadt,
     floor(sum(aadt*sec_len_miles)) as sum_vmt,
     sum(sec_len_miles*through_lanes) as sum_lane_miles,
     floor(sum(avg_single_unit*aadt/100)) as sum_single_unit,
     floor(sum(avg_single_unit*aadt*sec_len_miles/100)) as sum_single_unit_mt,
     floor(sum(avg_combination*aadt/100)) as sum_combination,
     floor(sum(avg_combination*aadt*sec_len_miles/100)) as sum_combination_mt
     from hpms_joined
     group by cell,year,route_number,f_system
     order by cell,year,f_system;

-- result:
--   cell  | year | route_number | f_system | sum_aadt | sum_vmt |  sum_lane_miles   | sum_single_unit | sum_single_unit_mt | sum_combination | sum_combination_mt
-- --------+------+--------------+----------+----------+---------+-------------------+-----------------+--------------------+-----------------+--------------------
--  189_72 | 2009 | 101          |        2 |    29000 |   20119 |  2.77508869053543 |               0 |                  0 |               0 |                  0
--  189_72 | 2009 | 101          |       12 |   267000 |   95487 |  4.29814414732388 |            6670 |               2581 |            8000 |               3252
--  189_72 | 2009 |              |       14 |   151196 |   93787 |   17.150024415496 |            5098 |               3383 |            1381 |                950
--  189_72 | 2009 | 192          |       14 |    12300 |   16001 |  2.60181767868063 |             369 |                480 |               0 |                  0
--  189_72 | 2009 | 225          |       14 |    22000 |    3747 | 0.340672814821636 |               0 |                  0 |               0 |                  0
--  189_72 | 2009 | 192          |       16 |     3100 |    4507 |  2.90801266914361 |              62 |                 90 |               0 |                  0
--  189_72 | 2009 |              |       16 |    92391 |   51935 |  11.3696098361967 |            1034 |                306 |             240 |                 97
--  189_72 | 2009 |              |       17 |   153724 |   61562 |   39.826748623163 |             547 |                130 |               0 |                  0
-- (8 rows)

-- same.  Maybe the fhwa cells are geocoded??  Maybe there are no fhwa links in this grid cell?



select count(*)
     --,st_length( ST_Intersection(geom4326, hg.geom)  ) as clipped_length
     from hpms.hpms_data hd
     left outer join hpms.hpms_link_geom hlg on (hlg.hpms_id=hd.id)
     where
     --          hd.fips = '59' and
     section_id ~ 'FHWA*' and
     state_code=6 and
     year_record=2009
     and hlg.geo_id is  null;

--yep, FHWA records are NOT geocoded.

-- spatialvds=# select count(*)
-- spatialvds-#      --,st_length( ST_Intersection(geom4326, hg.geom)  ) as clipped_length
-- spatialvds-#      from hpms.hpms_data hd
-- spatialvds-#      left outer join hpms.hpms_link_geom hlg on (hlg.hpms_id=hd.id)
-- spatialvds-#      where
-- spatialvds-#      --          hd.fips = '59' and
-- spatialvds-#      section_id ~ 'FHWA*' and
-- spatialvds-#      state_code=6 and
-- spatialvds-#      year_record=2009
-- spatialvds-#      and hlg.geo_id is  null;
--  count
-- -------
--    573
o-- (1 row)

-- So skipping the highway data is somewhat misleading
