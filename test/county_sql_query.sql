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
           ,upper(county) as county, locality,link_desc,from_name, to_name
           ,CASE WHEN is_metric>0
                 THEN section_length*0.621371
                 ELSE section_length
                 END as sec_len_miles
    from hpms.hpms_data hd
    where section_id !~ 'FHWA*'
    and state_code=6
)
, qury as (select fips,county,year,route_number,f_system, sum(aadt) as sum_aadt
       ,  floor(sum(aadt*sec_len_miles)) as sum_vmt
       , sum(sec_len_miles*through_lanes) as sum_lane_miles
       , floor(sum(avg_single_unit*aadt/100)) as sum_single_unit
       , floor(sum(avg_single_unit*aadt*sec_len_miles/100)) as sum_single_unit_mt
       , floor(sum(avg_combination*aadt/100)) as sum_combination
       , floor(sum(avg_combination*aadt*sec_len_miles/100)) as sum_combination_mt
    from hpmsgeo
    group by fips,county,year,route_number,f_system
    order by fips,county,year,f_system
)
select fips,county,year,sum(sum_vmt) as ave_daily_vmt,sum(sum_vmt) *365 as total_annual_vmt ,sum(sum_single_unit_mt) as total_daily_smt ,sum(sum_combination_mt) as total_daily_cmt from qury group by fips,county,year order by fips::int, year;
