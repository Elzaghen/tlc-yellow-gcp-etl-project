CREATE OR REPLACE TABLE `quantum-talent-450007-s3.tlc_yellow_2024_11.tbl_analytics` AS (
SELECT 
  f.trip_id,
  f.vendor_id,
  dt.tpep_pickup_datetime,
  dt.tpep_dropoff_datetime,
  f.passenger_count,
  f.trip_distance,
  rc.rate_code_name,
  pu.pu_borough,
  pu.pu_zone,
  d.do_borough,
  d.do_zone,
  p.payment_type_name,
  f.fare_amount,
  f.extra,
  f.mta_tax,
  f.tip_amount,
  f.tolls_amount,
  f.improvement_surcharge,
  f.congestion_surcharge,
  f.airport_fee,
  f.total_amount

FROM `quantum-talent-450007-s3.tlc_yellow_2024_11.fact_table` f
JOIN `quantum-talent-450007-s3.tlc_yellow_2024_11.dim_datetime` dt ON f.datetime_id = dt.datetime_id
JOIN `quantum-talent-450007-s3.tlc_yellow_2024_11.dim_dropoff` d ON f.dropoff_id = d.dropoff_id
JOIN `quantum-talent-450007-s3.tlc_yellow_2024_11.dim_payment_type` p ON f.payment_type_id = p.payment_type_id
JOIN `quantum-talent-450007-s3.tlc_yellow_2024_11.dim_pickup` pu ON f.pickup_id = pu.pickup_id
JOIN `quantum-talent-450007-s3.tlc_yellow_2024_11.dim_rate_code` rc ON f.rate_code_id = rc.rate_code_id)
;