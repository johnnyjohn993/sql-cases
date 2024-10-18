--CHANGE LOGS
-- 2024-06-20 | John Jayme | bulky_type is now based on chargeable_weight
-- 2024-06-24 | John Jayme | changed 4999 weight to 3999 as per new logic
DROP TABLE IF EXISTS phbi_ops.lm_all_oct_2024_part_1;
CREATE TABLE IF NOT EXISTS phbi_ops.lm_all_oct_2024_part_1 AS

WITH dates as (
  SELECT DISTINCT date_id as grass_date
  FROM 
  regbida_keyreports.dim_date
  WHERE 1=1
  --  and date_id BETWEEN DATE('2024-02-01') and CURRENT_DATE - INTERVAL '1' DAY
    and date_id BETWEEN DATE('2024-10-01') and date ('2024-10-15')
  -- and date_id = date '2024-02-22'

)

,ids AS (
  SELECT DISTINCT user_id
  --   ,operator_name
  -- ,operator as operator_name
  FROM 
  --   spx_mart.dwd_spx_order_tracking_tab_ri_ph_ro spx
  spx_mart.dwd_spx_fleet_order_tracking_ri_ph spx
  INNER JOIN dates d ON DATE (spx.grass_date) = DATE (d.grass_date)
  WHERE operator <> ''
  AND user_id IS NOT NULL
)

,a1 AS (
  SELECT shipment_id
  ,CASE 
  WHEN spx.user_id = 0
  THEN try_cast(substr(replace(spx.operator, '['), 1, position(']' IN replace(spx.operator, '[')) - 1) AS INT)
  ELSE spx.user_id
  END AS user_id
  ,status
  ,CASE  WHEN content_station_id IS NULL
  THEN try_cast(reverse(split(replace(replace(order_path, '['), ']'), ',')) [1] AS INT)
  ELSE content_station_id
  END AS content_station_id
  ,spx.grass_date
  FROM 
--   spx_mart.dwd_spx_order_tracking_tab_ri_ph_ro spx
  spx_mart.dwd_spx_fleet_order_tracking_ri_ph spx
  INNER JOIN dates d ON DATE (spx.grass_date) = DATE (d.grass_date)
  INNER JOIN ids ON spx.user_id = ids.user_id
  WHERE 1 = 1
  AND status IN (4,2,149,150
  ,50  -- for fed
  ,137 -- for fed
  )
  AND grass_region = 'PH'
  AND tz_type = 'local' --  and spx.user_id in (209253)
  AND spx.user_id IS NOT NULL
  AND spx.content_station_id IS NOT NULL
)

,transfer_history AS (
  SELECT driver_id
  ,transfer_date
  ,agency_history
  ,LEAD(transfer_date) OVER (PARTITION BY driver_id ORDER BY transfer_date ) AS next_transfer_date
  FROM 
  phbi_ops.spx_bi_driver_id_historical_agency_transfer_from_fms_extracted
)

,main AS (
  SELECT DISTINCT DATE (a.grass_date) AS status_date
  ,c2.region_gen driver_region
--   ,b.station_id AS parcel_hub_id
  ,b.dest_station_id AS parcel_hub_id
  ,a.content_station_id AS station_id -- station id of driver
  -- ,cs.station_id
  -- ,c2.area_cluster AS driver_subregion
  ,c2.subregion AS driver_subregion
  ,c2.hub AS station_name
  ,c.driver_id
  ,c.driver_name
  ,c.contract_type
  ,c.driver_status 
  ,c.fms_vehicle_type as fms_vehicle_type
  ,c.driver_group_name
  ,d.hub AS parcel_hub
  -- ,b.bulky_type
  ,case when b.chargeable_weight > 3999 then 1
  when b.chargeable_weight <= 3999 then 0
  when b.chargeable_weight is null then null
  end as bulky_type
  ,a.shipment_id
  ,a.status
 ,CASE 
  WHEN t.transfer_date IS NOT NULL 
  THEN TRIM(t.agency_history) 
  ELSE TRIM(c.agency) 
  END AS agency 
  FROM 
  a1 AS a -- cte 1
--   LEFT JOIN spx_mart.dwd_spx_fleet_order_tab_ri_ph_ro b ON a.shipment_id = b.shipment_id
  LEFT JOIN spx_mart.dwd_spx_fleet_order_ri_ph b ON a.shipment_id = b.shipment_id

  LEFT JOIN phbi_ops.spx_bi_driver_list c ON a.user_id = c.driver_id 
  -- LEFT JOIN dev_phbi_others.spx_station_lookup_official c2 ON a.content_station_id = c2.station_id
  -- LEFT JOIN dev_phbi_others.spx_station_lookup_official d ON b.dest_station_id = d.station_id
  LEFT JOIN phbi_ops.station_ids_and_station_name_from_import_range_version_2 c2 ON a.content_station_id = c2.station_id
  LEFT JOIN phbi_ops.station_ids_and_station_name_from_import_range_version_2 d ON b.dest_station_id = d.station_id

  LEFT JOIN transfer_history t ON c.driver_id = t.driver_id
  AND DATE (a.grass_date) >= t.transfer_date
  AND (
  DATE (a.grass_date) < t.next_transfer_date
  OR t.next_transfer_date IS NULL
  )
  WHERE b.channel_id = 1
  AND b.grass_region = 'PH'
  AND b.tz_type = 'local' -- and user_id in (102611)
  and c.fms_vehicle_type  <> '6WH'
  and c.fms_vehicle_type<> '10WH'
  and c.fms_vehicle_type <> 'Operator'
  and c.fms_vehicle_type <> 'Inhouse Checkers (Big Sellers)'
  and c.fms_vehicle_type <> 'Backroom'
  and c.fms_vehicle_type <> 'default'
  and c.fms_vehicle_type <> '8WH'
  and c.fms_vehicle_type <> '6WH'
  and c.fms_vehicle_type <> '6WF'

)

,a2 AS (
  SELECT a.grass_date
  ,a.shipment_id
  ,a.status
  ,min(from_unixtime(a.ctime)) first_feeding_time
  ,min_by(cast(json_extract(a.content, '$.pre_driver_id') AS INT), a.ctime) pre_driver_id -- ,min_by(cast(json_extract(a.content, '$.driver_id') as int),a.ctime) driver_id_fed
  FROM 
--   spx_mart.dwd_spx_order_tracking_tab_ri_ph_ro a
spx_mart.dwd_spx_fleet_order_tracking_ri_ph a
  INNER JOIN dates d ON DATE (a.grass_date) = DATE (d.grass_date)
  WHERE a.status IN (137,50)
  AND a.grass_region = 'PH'
  AND a.tz_type = 'local'
  GROUP BY 1,2,3
)

,fed AS (
  SELECT DATE (a.grass_date) fed_date
  ,c.driver_id
  ,c.driver_name 
  ,COALESCE(COUNT(DISTINCT CASE WHEN bulky_type = 1  AND a.status = 50 THEN a.shipment_id END), 0) AS fed_assigned_bulky
  ,COALESCE(COUNT(DISTINCT CASE WHEN bulky_type = 0  AND a.status = 50 THEN a.shipment_id END), 0) AS fed_assigned_non_bulky
  ,COALESCE(COUNT(DISTINCT CASE WHEN bulky_type = 1  AND a.status = 137 THEN a.shipment_id END), 0) AS fed_success_bulky
  ,COALESCE(COUNT(DISTINCT CASE WHEN bulky_type = 0  AND a.status = 137 THEN a.shipment_id END), 0) AS fed_success_non_bulky
  FROM a2 AS a 
  LEFT JOIN phbi_ops.spx_bi_driver_list c ON a.pre_driver_id = c.driver_id
--   LEFT JOIN spx_mart.dwd_spx_fleet_order_tab_ri_ph_ro b ON a.shipment_id = b.shipment_id
  LEFT JOIN spx_mart.dwd_spx_fleet_order_ri_ph b ON a.shipment_id = b.shipment_id
  WHERE b.channel_id = 1
  AND b.grass_region = 'PH'
  AND b.tz_type = 'local'
  AND a.pre_driver_id IN (
  SELECT DISTINCT driver_id
  FROM
  main
)
GROUP BY 1,2,3
)

SELECT DISTINCT 
  m.status_date
  ,m.driver_id
  -- ,m.driver_name -- ,license_number
  -- ,TRIM(CAST(m.driver_name AS varchar)) AS driver_name
  ,REGEXP_REPLACE(m.driver_name, '\s{2,}|^\s+|\s+$', ' ') AS driver_name
  ,m.agency
  ,m.station_id
  ,m.station_name
  ,m.parcel_hub_id
  ,m.parcel_hub
  ,m.driver_group_name -- ,m.contract_type
  ,CASE WHEN m.contract_type = 'Independent Contractor (IC)' THEN 'Independent Contractor (IC)' ELSE 'Subcontractor (Agency)'
  END AS contract_type
  ,m.driver_status -- ,m.fms_vehicle_type
  -- ,CASE WHEN fms_vehicle_type = '3WH' OR fms_vehicle_type = '3WL'
  -- THEN '3WC' WHEN m.fms_vehicle_type = '2WL'
  -- THEN '2WH' WHEN m.fms_vehicle_type = '4WL'
  -- THEN '4WH' ELSE m.fms_vehicle_type
  -- END AS fms_vehicle_type
  ,CASE WHEN fms_vehicle_type = '3WH' or fms_vehicle_type = '3WC' or fms_vehicle_type = 'Tricycle' then '3WC'
  WHEN fms_vehicle_type = '3WA' or fms_vehicle_type = '3WL' THEN '3WL'
  WHEN fms_vehicle_type = '2WL' THEN '2WH'
  WHEN fms_vehicle_type = '4WL'	or fms_vehicle_type = '4WAUV' or fms_vehicle_type = '4WCV'
  THEN '4WH'	ELSE fms_vehicle_type	END AS fms_vehicle_type
  ,m.driver_region
  ,m.driver_subregion
  -- Delivered
  ,COALESCE(COUNT(DISTINCT CASE WHEN bulky_type = 1 AND status in (2,149) THEN shipment_id END), 0) AS delivered_assigned_bulky,
  -- COALESCE(COUNT(DISTINCT CASE WHEN bulky_type <> 1 and status in (2,149) THEN shipment_id END), 0) AS delivered_assigned_non_bulky,
  COALESCE(COUNT(DISTINCT CASE WHEN bulky_type = 0 and status in (2,149) THEN shipment_id END), 0) AS delivered_assigned_non_bulky,
  COALESCE(COUNT(DISTINCT CASE WHEN bulky_type = 1 AND status in (4,150) THEN shipment_id END), 0) AS delivered_success_bulky,
  -- COALESCE(COUNT(DISTINCT CASE WHEN bulky_type <> 1 AND  status in (4,150) THEN shipment_id END), 0) AS delivered_success_non_bulky
  COALESCE(COUNT(DISTINCT CASE WHEN bulky_type = 0 AND  status in (4,150) THEN shipment_id END), 0) AS delivered_success_non_bulky

  -- Fed
  -- COALESCE(COUNT(DISTINCT CASE WHEN bulky_type = 1 AND status = 50 THEN shipment_id END), 0) AS fed_assigned_bulky,
  -- COALESCE(COUNT(DISTINCT CASE WHEN bulky_type <> 1 AND status = 50 THEN shipment_id END), 0) AS fed_assigned_non_bulky,
  -- COALESCE(COUNT(DISTINCT CASE WHEN bulky_type = 1 AND status = 137 THEN shipment_id END), 0) AS fed_success_bulky,
  -- COALESCE(COUNT(DISTINCT CASE WHEN bulky_type <> 1 AND status = 137 THEN shipment_id END), 0) AS fed_success_non_bulky,

  ,COALESCE(f.fed_assigned_bulky, 0) AS fed_assigned_bulky
  ,COALESCE(f.fed_assigned_non_bulky, 0) AS fed_assigned_non_bulky
  ,COALESCE(f.fed_success_bulky, 0) AS fed_success_bulky
  ,COALESCE(f.fed_success_non_bulky, 0) AS fed_success_non_bulky,
  --RTS
  0 AS rts_assigned_bulky,
  0 AS rts_assigned_non_bulky,
  0 AS rts_success_bulky,
  0 AS rts_success_non_bulky,
  -- PUS
  0 AS pick_up_from_seller_assigned_bulky,
  0 AS pick_up_from_seller_assigned_non_bulky,
  0 AS pick_up_from_seller_success_bulky,
  0 AS pick_up_from_seller_success_non_bulky,
  -- PUB
  0 AS pick_up_from_buyer_assigned_bulky,
  0 AS pick_up_from_buyer_assigned_non_bulky,
  0 AS pick_up_from_buyer_success_bulky,
  0 AS pick_up_from_buyer_success_non_bulky
--ROS
    ,0 AS ros_assigned_bulky
    ,0 AS ros_assigned_non_bulky
    ,0 AS ros_success_bulky
    ,0 AS ros_success_non_bulky

  from 
  main m

  left join fed f 
  on m.driver_id = f.driver_id 
  and m.status_date = f.fed_date 
  and m.driver_name = f.driver_name

  where m.driver_id is not null
  and agency <> 'Shopee Xpress'
  and m.fms_vehicle_type  <> '6WH'

  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12,13,14
  ,19,20,21,22