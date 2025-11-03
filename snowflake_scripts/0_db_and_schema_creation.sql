use role sysadmin;
use warehouse compute_wh;

create database if not exists yelp_db;
use database yelp_db;
create schema if not exists source;
create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;

use schema source;

-- create an internal stage and enable directory service
create stage if not exists landing_zone
directory = ( enable = true)
comment = 'all yelp related raw data will store in this internal stage location';

 -- create file format to process the JSON file
  create file format if not exists json_file_format 
      type = 'JSON'
      compression = 'AUTO' 
      comment = 'this is json file format object';

-- all the ETL workload will be manage by it.
create warehouse if not exists transform_wh
     comment = 'this is ETL warehouse for all loading activity' 
     warehouse_size = 'x-small' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;