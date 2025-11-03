use role sysadmin;
use warehouse compute_wh;

use database yelp_db;

-- 1. Create Bronze Tables
CREATE OR REPLACE TABLE bronze.business_raw (
    raw_data VARIANT,
    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

CREATE OR REPLACE TABLE bronze.reviews_raw (
    raw_data VARIANT,
    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

CREATE OR REPLACE TABLE bronze.users_raw (
    raw_data VARIANT,
    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);

list @source.landing_zone;

list @source.landing_zone/users;


-- Load the business file into bronze.business table
COPY INTO bronze.business_raw(raw_data, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
FROM (
    SELECT 
        $1 AS raw_data,
        METADATA$FILENAME AS _stg_file_name,
        METADATA$FILE_LAST_MODIFIED AS _stg_file_load_ts,
        METADATA$FILE_CONTENT_KEY AS _stg_file_md5,
        CURRENT_TIMESTAMP() AS _copy_data_ts
    FROM @source.landing_zone/business
)
FILE_FORMAT = (FORMAT_NAME = 'source.json_file_format')
ON_ERROR = CONTINUE;

-- Load the users file into bronze.user table
COPY INTO bronze.users_raw(raw_data, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
FROM (
    SELECT 
        $1 AS raw_data,
        METADATA$FILENAME AS _stg_file_name,
        METADATA$FILE_LAST_MODIFIED AS _stg_file_load_ts,
        METADATA$FILE_CONTENT_KEY AS _stg_file_md5,
        CURRENT_TIMESTAMP() AS _copy_data_ts
    FROM @source.landing_zone/users
)
FILE_FORMAT = (FORMAT_NAME = 'source.json_file_format')
ON_ERROR = CONTINUE;

 -- Load the reviews file into bronze.user table
COPY INTO bronze.reviews_raw(raw_data, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
FROM (
    SELECT 
        $1 AS raw_data,
        METADATA$FILENAME AS _stg_file_name,
        METADATA$FILE_LAST_MODIFIED AS _stg_file_load_ts,
        METADATA$FILE_CONTENT_KEY AS _stg_file_md5,
        CURRENT_TIMESTAMP() AS _copy_data_ts
    FROM @source.landing_zone/reviews
)
FILE_FORMAT = (FORMAT_NAME = 'source.json_file_format')
ON_ERROR = CONTINUE;

-- Validate data load
SELECT * FROM bronze.users_raw limit 10;

SELECT * FROM bronze.business_raw limit 10;

SELECT * FROM bronze.reviews_raw limit 10;