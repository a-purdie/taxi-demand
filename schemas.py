import pyarrow as pa

YELLOW_COLUMNS = [
    ("vendor_name", pa.string()),
    ("trip_pickup_datetime", pa.string()),
    ("trip_dropoff_datetime", pa.string()),
    ("passenger_count", pa.int64()),
    ("trip_distance", pa.float64()),
    ("start_lon", pa.float64()),
    ("start_lat", pa.float64()),
    ("rate_code", pa.string()),
    ("store_and_forward", pa.string()),
    ("end_lon", pa.float64()),
    ("end_lat", pa.float64()),
    ("payment_type", pa.string()),
    ("fare_amt", pa.float64()),
    ("surcharge", pa.float64()),
    ("mta_tax", pa.float64()),
    ("tip_amt", pa.float64()),
    ("tolls_amt", pa.float64()),
    ("total_amt", pa.float64()),
    ("pulocationid", pa.int64()),
    ("dolocationid", pa.int64()),
    ("improvement_surcharge", pa.float64()),
    ("congestion_surcharge", pa.float64()),
    ("airport_fee", pa.float64()),
    ("cbd_congestion_fee", pa.float64()),
]

YELLOW_CANONICAL_NAMES = [c[0] for c in YELLOW_COLUMNS]

# Maps parquet column names (lowercased) to canonical names
YELLOW_RENAMES = {
    # 2010+ renames
    "vendor_id": "vendor_name",
    "pickup_datetime": "trip_pickup_datetime",
    "dropoff_datetime": "trip_dropoff_datetime",
    "pickup_longitude": "start_lon",
    "pickup_latitude": "start_lat",
    "dropoff_longitude": "end_lon",
    "dropoff_latitude": "end_lat",
    "fare_amount": "fare_amt",
    "tip_amount": "tip_amt",
    "tolls_amount": "tolls_amt",
    "total_amount": "total_amt",
    "store_and_fwd_flag": "store_and_forward",
    # 2011+ renames
    "vendorid": "vendor_name",
    "tpep_pickup_datetime": "trip_pickup_datetime",
    "tpep_dropoff_datetime": "trip_dropoff_datetime",
    "ratecodeid": "rate_code",
    "extra": "surcharge",
}

YELLOW_DROP = {"__index_level_0__"}


# Green taxi canonical schema: (column_name, arrow_type)
# Consistent column names across all eras; cbd_congestion_fee added in 2025
GREEN_COLUMNS = [
    ("vendorid", pa.int64()),
    ("lpep_pickup_datetime", pa.string()),
    ("lpep_dropoff_datetime", pa.string()),
    ("store_and_fwd_flag", pa.string()),
    ("ratecodeid", pa.int64()),
    ("pulocationid", pa.int64()),
    ("dolocationid", pa.int64()),
    ("passenger_count", pa.int64()),
    ("trip_distance", pa.float64()),
    ("fare_amount", pa.float64()),
    ("extra", pa.float64()),
    ("mta_tax", pa.float64()),
    ("tip_amount", pa.float64()),
    ("tolls_amount", pa.float64()),
    ("ehail_fee", pa.float64()),
    ("improvement_surcharge", pa.float64()),
    ("total_amount", pa.float64()),
    ("payment_type", pa.int64()),
    ("trip_type", pa.int64()),
    ("congestion_surcharge", pa.float64()),
    ("cbd_congestion_fee", pa.float64()),
]


# FHV canonical schema
FHV_COLUMNS = [
    ("dispatching_base_num", pa.string()),
    ("pickup_datetime", pa.string()),
    ("dropoff_datetime", pa.string()),
    ("pulocationid", pa.float64()),
    ("dolocationid", pa.float64()),
    ("sr_flag", pa.string()),
    ("affiliated_base_number", pa.string()),
]

# FHVHV canonical schema
FHVHV_COLUMNS = [
    ("hvfhs_license_num", pa.string()),
    ("dispatching_base_num", pa.string()),
    ("originating_base_num", pa.string()),
    ("request_datetime", pa.string()),
    ("on_scene_datetime", pa.string()),
    ("pickup_datetime", pa.string()),
    ("dropoff_datetime", pa.string()),
    ("pulocationid", pa.int64()),
    ("dolocationid", pa.int64()),
    ("trip_miles", pa.float64()),
    ("trip_time", pa.int64()),
    ("base_passenger_fare", pa.float64()),
    ("tolls", pa.float64()),
    ("bcf", pa.float64()),
    ("sales_tax", pa.float64()),
    ("congestion_surcharge", pa.float64()),
    ("airport_fee", pa.float64()),
    ("tips", pa.float64()),
    ("driver_pay", pa.float64()),
    ("shared_request_flag", pa.string()),
    ("shared_match_flag", pa.string()),
    ("access_a_ride_flag", pa.string()),
    ("wav_request_flag", pa.string()),
    ("wav_match_flag", pa.string()),
]