DROP TABLE IF EXISTS phbi_ops.lm_all_mile_with_tags_invetigate;
CREATE TABLE IF NOT EXISTS phbi_ops.lm_all_mile_with_tags_invetigate AS

WITH TransferCTE AS (
    SELECT 
        *,
        LAG(driver_id) OVER (PARTITION BY driver_name ORDER BY status_date) AS prev_driver_id,
        LAG(agency) OVER (PARTITION BY driver_name ORDER BY status_date) AS prev_agency,
        LAG(fms_vehicle_type) OVER (PARTITION BY driver_name ORDER BY status_date) AS prev_fms_vehicle_type,
        -- LAG(station_name) OVER (PARTITION BY driver_name ORDER BY status_date) AS prev_station_name,
        LAG(station_name) OVER (PARTITION BY driver_name ORDER BY status_date) AS prev_station_name,
        LAG(parcel_hub) OVER (PARTITION BY driver_name ORDER BY status_date) AS prev_parcel_hub,
        LAG(driver_status) OVER (PARTITION BY driver_name ORDER BY status_date) AS prev_driver_status,
        LAG(driver_name) OVER (PARTITION BY driver_name ORDER BY status_date) AS prev_driver_name,
        LAG(driver_group_name) OVER (PARTITION BY driver_name ORDER BY status_date) AS prev_driver_group_name
    FROM 
      phbi_ops.driver_lm_investigate_norm_regions
),
base AS (
    SELECT 
        *,
        CASE WHEN (prev_driver_id IS NOT NULL AND prev_driver_id <> driver_id) THEN 1 ELSE 0 END AS is_driver_id_change,
        CASE WHEN (prev_agency IS NOT NULL AND prev_agency <> agency) THEN 1 ELSE 0 END AS is_agency_transfer,
        CASE WHEN (prev_fms_vehicle_type IS NOT NULL AND prev_fms_vehicle_type <> fms_vehicle_type) THEN 1 ELSE 0 END AS is_fms_vehicle_type_change,
        -- CASE WHEN (prev_station_name IS NOT NULL AND prev_station_name <> station_name) THEN 1 ELSE 0 END AS is_station_change,
        CASE WHEN (prev_station_name IS NOT NULL AND prev_station_name <> station_name) THEN 1 ELSE 0 END AS is_station_change,
        CASE WHEN (prev_parcel_hub IS NOT NULL AND prev_parcel_hub <> parcel_hub) THEN 1 ELSE 0 END AS is_parcel_hub_change,
        CASE WHEN (prev_driver_status IS NOT NULL AND prev_driver_status <> driver_status) THEN 1 ELSE 0 END AS is_driver_status_change,
        CASE WHEN (prev_driver_name IS NOT NULL AND prev_driver_name <> driver_name) THEN 1 ELSE 0 END AS is_driver_name_change,
        CASE WHEN (prev_driver_group_name IS NOT NULL AND prev_driver_group_name <> driver_group_name) THEN 1 ELSE 0 END AS is_driver_group_name_change
    FROM 
        TransferCTE
),
more_tag AS (  
   SELECT *,
    CASE 
        WHEN driver_name IN (
            SELECT DISTINCT driver_name 
            FROM base 
            WHERE is_driver_id_change = 1
        ) THEN 1
        ELSE 0
    END AS is_driver_id_change_all, -- column wise tag

    CASE 
        WHEN driver_name IN (
            SELECT DISTINCT driver_name 
            FROM base 
            WHERE is_agency_transfer = 1
        ) THEN 1
        ELSE 0
    END AS is_agency_transfer_all, -- column wise tag

    CASE 
        WHEN driver_name IN (
            SELECT DISTINCT driver_name 
            FROM base 
            WHERE is_fms_vehicle_type_change = 1
        ) THEN 1
        ELSE 0
    END AS is_fms_vehicle_type_change_all, -- column wise tag

    CASE 
        WHEN driver_name IN (
            SELECT DISTINCT driver_name 
            FROM base 
           WHERE is_station_change = 1
        ) THEN 1
        ELSE 0
    END AS is_station_change_all, -- column wise tag

    CASE 
        WHEN driver_name IN (
            SELECT DISTINCT driver_name 
            FROM base 
            WHERE is_parcel_hub_change = 1
        ) THEN 1
        ELSE 0
    END AS is_parcel_hub_change_all, -- column wise tag

    CASE 
        WHEN driver_name IN (
            SELECT DISTINCT driver_name 
            FROM base 
            WHERE is_driver_status_change = 1
        ) THEN 1
        ELSE 0
    END AS is_driver_status_change_all, -- column wise tag

    CASE 
        WHEN driver_name IN (
            SELECT DISTINCT driver_name 
            FROM base 
            WHERE is_driver_name_change = 1
        ) THEN 1
        ELSE 0
    END AS is_driver_name_change_all, -- column wise tag

    CASE 
        WHEN driver_name IN (
            SELECT DISTINCT driver_name 
            FROM base 
            WHERE is_driver_group_name_change = 1
        ) THEN 1
        ELSE 0
    END AS is_driver_group_name_change_all -- column wise tag
   FROM base
)

SELECT * FROM more_tag;
