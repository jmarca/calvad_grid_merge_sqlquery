with hpmsgrids as
  (select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
   --,st_centroid(grids.geom4326) as centroid
   --,grids.geom4326 as geom
   ,hd.hpms_id as hpms_id
   from carbgrid.state4k grids
   join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
   join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
   ),
hpmsgeo as
     (select
     id, year_record as year, state_Code,is_metric,fips,begin_lrs,end_lrs
     route_number, type_facility,section_length, aadt,through_lanes,
     lane_width, peak_parking,
     speed_limit, design_speed,
     perc_single_unit,avg_single_unit,
     perc_combination,avg_combination,
     k_factor,dir_factor,
     peak_lanes,peak_capacity,
     county, locality,link_desc,from_name, to_name
     ,hg.cell
     from hpms.hpms_data hd
     join hpmsgrids hg on (hd.id=hg.hpms_id)
     where section_id !~ 'FHWA*'
     )
select cell,year,type_facility,sum(aadt)
from hpmsgeo
group by cell,year,type_facility
;

-- work out recoding, so that sum aadt by facility type


-- anyway, that comes down lickety split

-- need to split out what road as well, because some highways inside  of grids are already covered by vds data.



with hpmsgrids as
  (select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
   --,st_centroid(grids.geom4326) as centroid
   --,grids.geom4326 as geom
   ,hd.hpms_id as hpms_id
   from carbgrid.state4k grids
   join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
   join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
   ),
hpmsgeo as
     (select
     id, year_record as year, state_Code,is_metric,fips,begin_lrs,end_lrs,
     route_number, type_facility,section_length, aadt,through_lanes,
     lane_width, peak_parking,
     speed_limit, design_speed,
     perc_single_unit,avg_single_unit,
     perc_combination,avg_combination,
     k_factor,dir_factor,
     peak_lanes,peak_capacity,
     county, locality,link_desc,from_name, to_name
     ,hg.cell
     from hpms.hpms_data hd
     join hpmsgrids hg on (hd.id=hg.hpms_id)
     where section_id !~ 'FHWA*'
     )
select cell,year,type_facility,route_number,sum(aadt)
from hpmsgeo
group by cell,year,type_facility,route_number
;


with hpmsgrids as
  (select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
   --,st_centroid(grids.geom4326) as centroid
   --,grids.geom4326 as geom
   ,hd.hpms_id as hpms_id
   from carbgrid.state4k grids
   join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
   join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
   ),
hpmsgeo as
     (select
     id, year_record as year, state_Code,is_metric,fips,begin_lrs,end_lrs,
     route_number, type_facility,section_length, aadt,through_lanes,
     lane_width, peak_parking,
     speed_limit, design_speed,
     perc_single_unit,avg_single_unit,
     perc_combination,avg_combination,
     k_factor,dir_factor,
     peak_lanes,peak_capacity,
     county, locality,link_desc,from_name, to_name
     ,hg.cell
     from hpms.hpms_data hd
     join hpmsgrids hg on (hd.id=hg.hpms_id)
     where section_id !~ 'FHWA*'
     and cell = '189_72'
     )
select *
from hpmsgeo
;

-- now to get grid cells overlapping current calvad data.



with alldetectors as
  (select distinct floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell,st_centroid(grids.geom4326) as centroid, grids.geom4326 as geom
   from carbgrid.state4k grids
   join tempseg.tdetector ttd on  st_intersects(ttd.seggeom,grids.geom4326)
   ),
countygrids as
  (select distinct c.cell, '"county":"'||a.name||'","fips":"'|| conum ||'"' as countystr
   from alldetectors c
   join public.carb_counties_aligned_03 a on (st_contains(a.geom4326,c.centroid))
   ),
districtgrids as
  (select distinct c.cell, '"airdistrict":"'||a.disn||'","dis":"'|| a.dis ||'"' as districtstr
   from alldetectors c
   join public.carb_airdistricts_aligned_03 a on (st_contains(a.geom4326,c.centroid))
   )
select  '"'|| cell ||'":{'|| basinstr || ',' || countystr || ',' || districtstr ||  '},' as jsonstr
from basingrids
left outer join countygrids using (cell)
left outer join districtgrids using (cell)
order by cell,jsonstr
;

select distinct floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell,st_centroid(grids.geom4326) as centroid, grids.geom4326 as geom
   from carbgrid.state4k grids
   join tempseg.tdetector ttd on  st_intersects(ttd.geom,grids.geom4326)
   where cell='189_72'

with alldetectors as
  (
select distinct floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
  ,st_centroid(grids.geom4326) as centroid, grids.geom4326 as geom
   from carbgrid.state4k grids
   join tempseg.mostusedroadbits murb on st_intersects(seggeom,grids.geom4326)
   where i_cell=189 and j_cell=72

)



select section_id,year_record,section_length, is_metric,
       CASE WHEN is_metric>0 THEN section_length*0.621371
            ELSE section_length
            END as sec_len_miles
from hpms.hpms_data hd
order by section_id,year_record;


with hpmsgrids as
  (select distinct floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
   ,hd.hpms_id as hpms_id
   from carbgrid.state4k grids
   join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
   join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
   where i_cell=189 and j_cell=72
),
hpmsgeo as
     (select
     id, year_record as year, state_code,is_metric,fips,begin_lrs,end_lrs,
     route_number, type_facility,section_length, aadt,through_lanes,
     lane_width, peak_parking,
     speed_limit, design_speed,
     perc_single_unit,avg_single_unit,
     perc_combination,avg_combination,
     k_factor,dir_factor,
     peak_lanes,peak_capacity,
     county, locality,link_desc,from_name, to_name
     ,hg.cell
     ,CASE WHEN is_metric>0 THEN section_length*0.621371
           ELSE section_length
      END as sec_len_miles
     from hpms.hpms_data hd
     join hpmsgrids hg on (hd.id=hg.hpms_id)
     where section_id !~ 'FHWA*'
)
select cell,year,type_facility,route_number,sum(aadt) as sum_aadt,
       floor(sum(aadt*sec_len_miles)) as sum_vmt,
       sum(sec_len_miles*through_lanes) as sum_lane_miles
from hpmsgeo
group by cell,year,type_facility,route_number
order by cell,year
;




-- scratch for selecting the highway numbers with detectors on them
-- (existing in calvad, theoretically)

with alldetectors as
  (
select distinct floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
  ,st_centroid(grids.geom4326) as centroid, grids.geom4326 as geom
   from carbgrid.state4k grids
   join tempseg.mostusedroadbits murb on st_intersects(seggeom,grids.geom4326)
   where i_cell=189 and j_cell=72

)


select distinct floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
       ,refnum as route_number
from carbgrid.state4k grids
join tempseg.tdetector ttd on st_intersects(ttd.geom,grids.geom4326)
where i_cell=189 and j_cell = 72   ;


-- fiddle about.  This is what the current query looks like:

with hpmsgrids as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
            ,hd.hpms_id as hpms_id
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where floor(grids.i_cell) || '_'|| floor(grids.j_cell)='189_72'
),
hpmsgeo as (
    select id, year_record as year,
        state_code,is_metric,fips,begin_lrs,end_lrs,
        route_number, type_facility,f_system,gf_system,section_length,
        aadt,through_lanes,
        lane_width, peak_parking,
        speed_limit, design_speed,
        perc_single_unit,coalesce(avg_single_unit,0.0) as avg_single_unit,
        perc_combination,coalesce(avg_combination,0.0) as avg_combination,
        k_factor,dir_factor,
        peak_lanes,peak_capacity,
        county, locality,link_desc,from_name, to_name
        ,hg.cell
        ,CASE WHEN is_metric>0 THEN section_length*0.621371
              ELSE section_length END as sec_len_miles
    from hpms.hpms_data hd
    join hpmsgrids hg on (hd.id=hg.hpms_id)
    where section_id !~ 'FHWA*'
        and state_code=6
        and year_record=2009
)
select cell,year,route_number,f_system, sum(aadt) as sum_aadt,
       floor(sum(aadt*sec_len_miles)) as sum_vmt,
       sum(sec_len_miles*through_lanes) as sum_lane_miles  ,
       floor(sum(avg_single_unit*aadt/100)) as sum_daily_single_unit  ,
       floor(sum(avg_single_unit*aadt*sec_len_miles/100)) as sum_daily_single_unit_mt  ,
       floor(sum(avg_combination*aadt/100)) as sum_daily_combination,
       floor(sum(avg_combination*aadt*sec_len_miles/100)) as sum_daily_combination_mt
from hpmsgeo
group by cell,year,route_number,f_system
order by cell,year,f_system
;


-- figure out what to do really with trucks

with hpmsgrids as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
            ,hd.hpms_id as hpms_id
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where floor(grids.i_cell) || '_'|| floor(grids.j_cell)='189_72'
),
hpmsgeo as (
    select id, year_record as year,
        state_code,is_metric,fips,begin_lrs,end_lrs,
        route_number, type_facility,f_system,gf_system,section_length,
        aadt,through_lanes,
        lane_width, peak_parking,
        speed_limit, design_speed,
        perc_single_unit,coalesce(avg_single_unit,0.0) as avg_single_unit,
        perc_combination,coalesce(avg_combination,0.0) as avg_combination,
        k_factor,dir_factor,
        peak_lanes,peak_capacity,
        county, locality,link_desc,from_name, to_name
        ,hg.cell
        ,CASE WHEN is_metric>0 THEN section_length*0.621371
              ELSE section_length END as sec_len_miles
    from hpms.hpms_data hd
    join hpmsgrids hg on (hd.id=hg.hpms_id)
    where section_id !~ 'FHWA*'
        and state_code=6
        and year_record=2009
)
select cell,year,route_number,f_system, aadt,
       floor((aadt*sec_len_miles)) as vmt,
       sec_len_miles*through_lanes as lane_miles  ,
       avg_single_unit as pct_daily_single_unit  ,
       avg_combination as pct_daily_combination
from hpmsgeo
--group by cell,year,route_number,f_system
order by cell,year,f_system
;


-- pct needs to be divided by 100, and make sure to get truck miles traveled


-- now get all those hpms records taht are not yet associated with a geo code


-- work out checking if there is double counting going on

with hpmsgrids_all as (
    select  floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
            ,hd.hpms_id as hpms_id
            ,st_length((ST_Dump(ST_Intersection(grids.geom4326, hg.geom))).geom)/st_length(hg.geom) as clipped_fraction
    from carbgrid.state4k grids
    join hpms.hpms_geom hg on st_intersects(grids.geom4326,hg.geom)
    join hpms.hpms_link_geom hd on (hg.id=hd.geo_id)
    where floor(grids.i_cell) || '_'|| floor(grids.j_cell)='189_72'
),
hpmsgrids as (select * from hpmsgrids_all where clipped_fraction > 0.01),



with
grid_cell as (   select floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell          ,geom4326   from carbgrid.state4k grids   where (floor(grids.i_cell) || '_'|| floor(grids.j_cell)='189_72') ),
fips_code as (   select (regexp_matches(fips, '060*(d*)'))[1] as fips   from counties_fips   where upper(name)='SANTA BARBARA' ),
hpms_grid_county as (   select grid_cell.cell,          hd.gid as hpms_id, year_record as year,          state_code,fips,      route_id,      aadt,          section_length as full_sec_len_miles          ,st_transform(geom,4326) as geom          ,st_length( ST_Intersection(geom4326, st_transform(geom,4326))  ) as clipped_length          ,regexp_split_to_array(route_id,E'_') as name_parts   from grid_cell   join hpms.hpms_2014 hd  on ( st_intersects(geom4326,st_transform(geom,4326)) )   join fips_code fc on (1=1)   where      year_record=2014 ),
hpmsgrids_summed as (   select cell, hpms_id,          sum(clipped_length) as clipped_length   from hpms_grid_county   group by cell, hpms_id ),
hpms_links as (   select distinct hpms_id   from hpmsgrids_summed ),
hpms_only as (   select hpms_id          ,st_length(hg.geom) as orig_length   from hpms_links hgs   join hpms.hpms_2014 hg on (hg.gid = hgs.hpms_id) ),
hpms_fractional as (   select cell,hpms_id,clipped_length,orig_length,clipped_length/orig_length as clipped_fraction   from hpmsgrids_summed hgs   join hpms_only htl using (hpms_id) ),
hpms_fractional_limited as (   select cell,hpms_id, clipped_fraction   from hpms_fractional   where clipped_fraction > 0.01 ),
names_cities as (   select hgc.*,   case     when array_length(name_parts,1) = 4 then upper(name_parts[2])     when name_parts[1] ~* 'SHS' then 'SHS'     else name_parts[1]   end as city_abbrev   from hpms_grid_county hgc ),
city_join as (   select hgc.*,coalesce(ca.name,hgc.city_abbrev) as city_name   from names_cities hgc   left outer join geocoding.city_abbrev ca on (upper(ca.abbrev)=hgc.city_abbrev) ),
hpms_joined as (   select hga.*, clipped_fraction, clipped_fraction*full_sec_len_miles as sec_len_miles ,case   when array_length(name_parts,1) = 4 then city_name || ' ' || name_parts[4]   when name_parts[1] ~* 'SHS' then route_id   else name_parts[1] end as classify ,case   when name_parts[1] ~* 'SHS' then name_parts[2] end as route_number   from city_join hga   join hpms_fractional_limited hfl on (hga.hpms_id=hfl.hpms_id) )
select cell,year,classify as f_system,route_number,
       aadt,
       aadt*sec_len_miles as vmt
from hpms_joined
order by cell,year,classify,route_number

  cell  | year |    f_system     | route_number |  aadt  |    vmt
--------+------+-----------------+--------------+--------+-----------
 189_72 | 2014 | CO P            |              |    453 |    281.01
 189_72 | 2014 | CO P            |              |   1527 |    395.49
 189_72 | 2014 | CO P            |              |   8484 |  13691.23
 189_72 | 2014 | CO P            |              |   7071 |    754.86
 189_72 | 2014 | CO P            |              |  11118 |   2523.79
 189_72 | 2014 | CO P            |              |   9611 |   2840.37
 189_72 | 2014 | CO P            |              |    316 |    183.91
 189_72 | 2014 | CO P            |              |   2269 |    578.60
 189_72 | 2014 | CO P            |              |   1663 |   2173.54
 189_72 | 2014 | CO P            |              |    673 |    286.70
 189_72 | 2014 | CO P            |              |  11967 |   2246.23
 189_72 | 2014 | CO P            |              |  26938 |   8859.82
 189_72 | 2014 | CO P            |              |  23955 |   8072.84
 189_72 | 2014 | CO P            |              |  31081 |   7583.76
 189_72 | 2014 | CO P            |              |  20301 |   7003.85
 189_72 | 2014 | CO P            |              |  11748 |  12687.84
 189_72 | 2014 | CO P            |              |   2400 |    583.20

 189_72 | 2014 | RAMP            |              |   6414 |    833.82
 189_72 | 2014 | RAMP            |              |   5857 |    585.70
 189_72 | 2014 | RAMP            |              |   7761 |   1629.81

 189_72 | 2014 | Santa Barbara P |              |   1959 |    495.63
 189_72 | 2014 | Santa Barbara P |              |  11768 |   5154.38
 189_72 | 2014 | Santa Barbara P |              |   4164 |    324.79
 189_72 | 2014 | Santa Barbara P |              |   8196 |   4811.05
 189_72 | 2014 | Santa Barbara P |              |   7686 |   1908.75
 189_72 | 2014 | Santa Barbara P |              |   4889 |   2776.95
 189_72 | 2014 | Santa Barbara P |              |   2132 |    211.07
 189_72 | 2014 | Santa Barbara P |              |   2132 |    829.35
 189_72 | 2014 | Santa Barbara P |              |   4889 |   1461.81
 189_72 | 2014 | Santa Barbara P |              |   2562 |    753.23
 189_72 | 2014 | Santa Barbara P |              |  10977 |   2656.43
 189_72 | 2014 | Santa Barbara P |              |   5202 |    338.13
 189_72 | 2014 | Santa Barbara P |              |   5202 |    218.48
 189_72 | 2014 | Santa Barbara P |              |   3667 |   1410.18
 189_72 | 2014 | Santa Barbara P |              |   3667 |    355.70
 189_72 | 2014 | Santa Barbara P |              |   7226 |    715.37
 189_72 | 2014 | Santa Barbara P |              |   7226 |    708.15
 189_72 | 2014 | Santa Barbara P |              |   6525 |   1539.90
 189_72 | 2014 | Santa Barbara P |              |   4889 |   2385.83
 189_72 | 2014 | Santa Barbara P |              |   4889 |    415.57
 189_72 | 2014 | Santa Barbara P |              |   1348 |    143.11
 189_72 | 2014 | Santa Barbara P |              |   1348 |    261.51
 189_72 | 2014 | Santa Barbara P |              |   1566 |    115.61
 189_72 | 2014 | Santa Barbara P |              |   1857 |    183.84
 189_72 | 2014 | Santa Barbara P |              |   1857 |   1106.77
 189_72 | 2014 | Santa Barbara P |              |   4889 |    869.01
 189_72 | 2014 | Santa Barbara P |              |    702 |     94.77
 189_72 | 2014 | Santa Barbara P |              |   8131 |   1162.73
 189_72 | 2014 | Santa Barbara P |              |   5212 |   5112.97
 189_72 | 2014 | Santa Barbara P |              |   7161 |   1770.65
 189_72 | 2014 | Santa Barbara P |              |   1293 |    226.28
 189_72 | 2014 | Santa Barbara P |              |    598 |    169.83
 189_72 | 2014 | Santa Barbara P |              |    660 |    325.61
 189_72 | 2014 | Santa Barbara P |              |   3919 |   2676.68
 189_72 | 2014 | Santa Barbara P |              |   5777 |    929.94
 189_72 | 2014 | Santa Barbara P |              |   4889 |     48.89
 189_72 | 2014 | Santa Barbara P |              |  20100 |  10009.80
 189_72 | 2014 | Santa Barbara P |              |  19341 |   6885.40
 189_72 | 2014 | Santa Barbara P |              |    964 |    284.38
 189_72 | 2014 | Santa Barbara P |              |   4889 |    113.10
 189_72 | 2014 | Santa Barbara P |              |   8131 |   1235.91
 189_72 | 2014 | Santa Barbara P |              |   8131 |   1244.04
 189_72 | 2014 | Santa Barbara P |              |    474 |     89.05
 189_72 | 2014 | Santa Barbara P |              |   1862 |    368.68
 189_72 | 2014 | Santa Barbara P |              |   1293 |   1034.40
 189_72 | 2014 | Santa Barbara P |              |   1293 |      5.97
 189_72 | 2014 | Santa Barbara P |              |   3191 |   1017.93
 189_72 | 2014 | Santa Barbara P |              |   5404 |   2058.92
 189_72 | 2014 | Santa Barbara P |              |   5404 |     64.85
 189_72 | 2014 | Santa Barbara P |              |   4889 |    542.68
 189_72 | 2014 | Santa Barbara P |              |   2847 |    776.54
 189_72 | 2014 | Santa Barbara P |              |    965 |     48.87
 189_72 | 2014 | Santa Barbara P |              |    587 |     64.57
 189_72 | 2014 | Santa Barbara P |              |    673 |    141.33
 189_72 | 2014 | Santa Barbara P |              |    664 |     59.76
 189_72 | 2014 | Santa Barbara P |              |    673 |    190.46
 189_72 | 2014 | Santa Barbara P |              |   1003 |    478.43
 189_72 | 2014 | Santa Barbara P |              |   3919 |    281.58
 189_72 | 2014 | Santa Barbara P |              |   3750 |   2332.50
 189_72 | 2014 | Santa Barbara P |              |    496 |    357.62
 189_72 | 2014 | Santa Barbara P |              |   8320 |  10506.20
 189_72 | 2014 | Santa Barbara P |              |   4889 |     24.45
 189_72 | 2014 | Santa Barbara P |              |   1939 |    188.08
 189_72 | 2014 | Santa Barbara P |              |   1566 |    640.49
 189_72 | 2014 | Santa Barbara P |              |   1959 |    599.45
 189_72 | 2014 | Santa Barbara P |              |   1855 |    259.70
 189_72 | 2014 | Santa Barbara P |              |   2452 |   1257.88
 189_72 | 2014 | Santa Barbara P |              |   6518 |   4953.68
 189_72 | 2014 | Santa Barbara P |              |   4044 |    695.33
 189_72 | 2014 | Santa Barbara P |              |   1621 |    551.14
 189_72 | 2014 | Santa Barbara P |              |   1293 |    502.98
 189_72 | 2014 | Santa Barbara P |              |    659 |     36.90
 189_72 | 2014 | Santa Barbara P |              |    659 |     30.97
 189_72 | 2014 | Santa Barbara P |              |    659 |     96.87
 189_72 | 2014 | Santa Barbara P |              |    659 |    193.09
 189_72 | 2014 | Santa Barbara P |              |  21321 |   1145.04
 189_72 | 2014 | Santa Barbara P |              |  21868 |   3498.88
 189_72 | 2014 | Santa Barbara P |              |  12434 |   2437.06
 189_72 | 2014 | Santa Barbara P |              |   1270 |  10799.26
 189_72 | 2014 | Santa Barbara P |              |   1283 |    794.18
 189_72 | 2014 | Santa Barbara P |              |   2606 |    425.00
 189_72 | 2014 | Santa Barbara P |              |    673 |    174.31
 189_72 | 2014 | Santa Barbara P |              |   6261 |     62.61
 189_72 | 2014 | Santa Barbara P |              |   7516 |   3457.36
 189_72 | 2014 | Santa Barbara P |              |   7516 |   1965.83
 189_72 | 2014 | Santa Barbara P |              |   2606 |    255.39
 189_72 | 2014 | Santa Barbara P |              |   3043 |   1761.90
 189_72 | 2014 | Santa Barbara P |              |    470 |      5.64
 189_72 | 2014 | Santa Barbara P |              |    341 |     36.83
 189_72 | 2014 | Santa Barbara P |              |   5112 |   3037.69
 189_72 | 2014 | Santa Barbara P |              |   4376 |    870.82
 189_72 | 2014 | Santa Barbara P |              |   4859 |    971.80
 189_72 | 2014 | Santa Barbara P |              |   6351 |    861.59
 189_72 | 2014 | Santa Barbara P |              |   4889 |    356.90
 189_72 | 2014 | Santa Barbara P |              |   1959 |    193.94
 189_72 | 2014 | Santa Barbara P |              |   1000 |    171.00
 189_72 | 2014 | Santa Barbara P |              |  13482 |  17044.41
 189_72 | 2014 | Santa Barbara P |              |   2663 |    516.62
 189_72 | 2014 | Santa Barbara P |              |   4043 |    926.45
 189_72 | 2014 | Santa Barbara P |              |   4558 |   3085.77
 189_72 | 2014 | Santa Barbara P |              |   5567 |   2037.21
 189_72 | 2014 | SHS_101         | 101          | 134000 |  78828.22
 189_72 | 2014 | SHS_101         | 101          | 131500 | 104180.62
 189_72 | 2014 | SHS_192         | 192          |   9225 |   4986.85
 189_72 | 2014 | SHS_192         | 192          |   5700 |  12212.54
 189_72 | 2014 | SHS_225         | 225          |  22500 |   2301.08
