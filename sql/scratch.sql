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
