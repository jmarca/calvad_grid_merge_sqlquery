with names as (
  select route_id,regexp_split_to_array(route_id,E'_') as name_parts
  from hpms.hpms_2014
),
names_cities as (
  select route_id,
  name_parts,
  case
    when array_length(name_parts,1) = 4 then upper(name_parts[2])
    when name_parts[1] ~* 'SHS' then 'SHS'
    else name_parts[1]
  end as city_abbrev
  from names
),
city_join as (
  select route_id,name_parts,city_abbrev,ca.name as city_name
  from names_cities n
  left outer join geocoding.city_abbrev ca on (upper(ca.abbrev)=n.city_abbrev)
)
select
  case
  when array_length(name_parts,1) = 4 then name_parts[4]
  when name_parts[1] ~* 'SHS' then route_id
  else name_parts[1]
end as classify,
  city_abbrev,
  coalesce(city_name,city_abbrev) as city_name
    ,count(*)
  from city_join
  group by classify,city_abbrev,city_name
  order by count desc
;
