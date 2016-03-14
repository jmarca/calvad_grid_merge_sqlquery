with grid_cell as (
  select floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell
         ,geom4326
  from carbgrid.state4k grids
  where (floor(grids.i_cell) || '_'|| floor(grids.j_cell)='189_72')
),
fips_code as (
  select (regexp_matches(fips, '060*(\d*)'))[1] as fips
  from counties_fips
  where upper(name)='SANTA BARBARA'
),
hpms_grid_county as (
  select grid_cell.cell,-- grid_cell.geom4326,
         hd.gid as hpms_id, year_record as year,
         state_code,fips,      route_id,      aadt,
         section_length as full_sec_len_miles
         ,st_transform(geom,4326) as geom
         ,st_length( ST_Intersection(geom4326, st_transform(geom,4326))  ) as clipped_length
         ,regexp_split_to_array(route_id,E'_') as name_parts
  from grid_cell
  join hpms.hpms_2014 hd  on ( st_intersects(geom4326,st_transform(geom,4326)) )
  join fips_code fc on (1=1)
  where -- section_id !~ 'FHWA*'
    year_record=2014
),
hpmsgrids_summed as (
  select cell, hpms_id,
         sum(clipped_length) as clipped_length
  from hpms_grid_county
  group by cell, hpms_id
),
hpms_links as (
  select distinct hpms_id
  from hpmsgrids_summed
),
hpms_only as (
  select hpms_id
         ,st_length(hg.geom) as orig_length
  from hpms_links hgs
  join hpms.hpms_2014 hg on (hg.gid = hgs.hpms_id)
),
hpms_fractional as (
  select cell,hpms_id,clipped_length,orig_length,clipped_length/orig_length as clipped_fraction
  from hpmsgrids_summed hgs
  join hpms_only htl using (hpms_id)
),
hpms_fractional_limited as (
  select cell,hpms_id, clipped_fraction
  from hpms_fractional
  where clipped_fraction > 0.01
),
hpms_joined as (
  select hga.*, clipped_fraction, clipped_fraction*full_sec_len_miles as sec_len_miles
,case
  when array_length(name_parts,1) = 4 then name_parts[4]
  when name_parts[1] ~* 'SHS' then route_id
  else name_parts[1]
end as classify
  from hpms_grid_county hga
  join hpms_fractional_limited hfl on (hga.hpms_id=hfl.hpms_id)
)
select cell,year,classify,
       -- f_system,
       -- hpms_id,route_id,
       sum(aadt) as sum_aadt,
       floor(sum(aadt*sec_len_miles)) as sum_vmt
--       ,sum(sec_len_miles*through_lanes) as sum_lane_miles
--       ,floor(sum(avg_single_unit*aadt/100)) as sum_single_unit
--       ,floor(sum(avg_single_unit*aadt*sec_len_miles/100)) as sum_single_unit_mt
--       ,floor(sum(avg_combination*aadt/100)) as sum_combination
--       ,floor(sum(avg_combination*aadt*sec_len_miles/100)) as sum_combination_mt
from hpms_joined
group by cell,year,classify
order by cell,year,classify
;


with grid_cell as (   select floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell          ,geom4326   from carbgrid.state4k grids   where (floor(grids.i_cell) || '_'|| floor(grids.j_cell)='189_72') ),
fips_code as (   select (regexp_matches(fips, '060*(d*)'))[1] as fips   from counties_fips   where upper(name)='SANTA BARBARA' ),
hpms_grid_county as (   select grid_cell.cell,          hd.gid as hpms_id, year_record as year,          state_code,fips,      route_id,      aadt,          section_length as full_sec_len_miles          ,st_transform(geom,4326) as geom          ,st_length( ST_Intersection(geom4326, st_transform(geom,4326))  ) as clipped_length          ,regexp_split_to_array(route_id,E'_') as name_parts   from grid_cell   join hpms.hpms_2014 hd  on ( st_intersects(geom4326,st_transform(geom,4326)) )   join fips_code fc on (1=1)   where      year_record=2014 ),
hpmsgrids_summed as (   select cell, hpms_id,          sum(clipped_length) as clipped_length   from hpms_grid_county   group by cell, hpms_id ),
hpms_links as (   select distinct hpms_id   from hpmsgrids_summed ),
hpms_only as (   select hpms_id          ,st_length(hg.geom) as orig_length   from hpms_links hgs   join hpms.hpms_2014 hg on (hg.gid = hgs.hpms_id) ),
hpms_fractional as (   select cell,hpms_id,clipped_length,orig_length,clipped_length/orig_length as clipped_fraction   from hpmsgrids_summed hgs   join hpms_only htl using (hpms_id) ),
hpms_fractional_limited as (   select cell,hpms_id, clipped_fraction   from hpms_fractional   where clipped_fraction > 0.01 ),
hpms_joined as (   select hga.*, clipped_fraction, clipped_fraction*full_sec_len_miles as sec_len_miles ,case   when array_length(name_parts,1) = 4 then name_parts[4]   when name_parts[1] ~* 'SHS' then route_id   else name_parts[1] end as classify   from hpms_grid_county hga   join hpms_fractional_limited hfl on (hga.hpms_id=hfl.hpms_id) )
select cell,year,classify,         sum(aadt) as sum_aadt,         floor(sum(aadt*sec_len_miles)) as sum_vmt  from hpms_joined  group by cell,year,classify  order by cell,year,classify;


with grid_cell as (   select floor(grids.i_cell) || '_'|| floor(grids.j_cell) as cell          ,geom4326   from carbgrid.state4k grids
where (floor(grids.i_cell) || '_'|| floor(grids.j_cell)='189_72') ), fips_code as (   select (regexp_matches(fips, '060*(d*)'))[1] as fips
from counties_fips   where upper(name)='SANTA BARBARA' ), hpms_grid_county as (   select grid_cell.cell,          hd.gid as hpms_id, year_record as year,          state_code,fips,      route_id,      aadt,          section_length as full_sec_len_miles          ,st_transform(geom,4326) as geom          ,st_length( ST_Intersection(geom4326, st_transform(geom,4326))  ) as clipped_length          ,regexp_split_to_array(route_id,E'_') as name_parts
from grid_cell   join hpms.hpms_2014 hd  on ( st_intersects(geom4326,st_transform(geom,4326)) )   join fips_code fc on (1=1)
where      year_record=2014 ),hpmsgrids_summed as (   select cell, hpms_id,          sum(clipped_length) as clipped_length
from hpms_grid_county   group by cell, hpms_id ), hpms_links as (   select distinct hpms_id   from hpmsgrids_summed ),
hpms_only as (   select hpms_id          ,st_length(hg.geom) as orig_length   from hpms_links hgs
join hpms.hpms_2014 hg on (hg.gid = hgs.hpms_id) ),
hpms_fractional as (   select cell,hpms_id,clipped_length,orig_length,clipped_length/orig_length as clipped_fraction
from hpmsgrids_summed hgs   join hpms_only htl using (hpms_id) ),
hpms_fractional_limited as (   select cell,hpms_id, clipped_fraction   from hpms_fractional
where clipped_fraction > 0.01 ),
names_cities as (   select hgc.*,
case     when array_length(name_parts,1) = 4 then upper(name_parts[2])     when name_parts[1] ~* 'SHS' then 'SHS'     else name_parts[1]   end as city_abbrev
from hpms_grid_county hgc ), city_join as (   select hgc.*,ca.name as city_name   from names_cities hgc   left outer join geocoding.city_abbrev ca on (upper(ca.abbrev)=hgc.city_abbrev) ), hpms_joined as (   select hga.*, clipped_fraction, clipped_fraction*full_sec_len_miles as sec_len_miles ,case   when array_length(name_parts,1) = 4 then city_name || ' ' || name_parts[4]   when name_parts[1] ~* 'SHS' then route_id   else name_parts[1] end as classify   from city_join hga   join hpms_fractional_limited hfl on (hga.hpms_id=hfl.hpms_id) )

select hj.* from hpms_joined hj where classify is null;

select cell,year,classify,         sum(aadt) as sum_aadt,         floor(sum(aadt*sec_len_miles)) as sum_vmt  from hpms_joined  group by cell,year,classify  order by cell,year,classify
