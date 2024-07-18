/*
-- Poppy Analytics Pipeline. 
The pipeline to create automated Poppy Google review analytics
*/

USE ROLE sysadmin;

-- Database, schema and warehouse creation

-- create poppy database
CREATE OR REPLACE DATABASE poppy;

-- create raw_pos schema
CREATE OR REPLACE SCHEMA poppy.analytics;

USE POPPY.ANALYTICS;

-- create tasty_ds_wh warehouse
CREATE OR REPLACE WAREHOUSE poppy_wh
    WAREHOUSE_SIZE = 'x-small'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60  -- seconds
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data pipeline & analytics for Poppy Kids';


USE WAREHOUSE poppy_wh;

-- CREATE STAGE
USE SCHEMA ANALYTICS;
SHOW STAGES; -- Jay: Delete unused stages

DESCRIBE STAGE MY_PACKAGE_STAGE;

CREATE OR REPLACE STAGE my_package_stage; -- to store outscraper package

/*Build the Outscraper python package to stage using add data: https://pypi.org/project/outscraper/
-- Use visual studio vs code
-- pip install --upgrade outscraper
-- snow snowpark package create outscraper
-- Download from VSCode and upload to stage
*/

LIST @MY_PACKAGE_STAGE;


--------CREATE LANDING TABLE WITH RAW_DATA--------------
--------------------------------------------------------

create or replace TABLE POPPY.ANALYTICS.GOOGLE_REVIEWS_JSON (
	PLACE_ID VARCHAR(16777216),
	FETCH_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	RAW_DATA VARIANT
);

-- Test:
SELECT * FROM POPPY.ANALYTICS.GOOGLE_REVIEWS_JSON;

------------------SECRETS--------------------------
---------------------------------------------------
//DROP SCHEMA SECRETS;
USE ROLE ACCOUNTADMIN;
USE DATABASE POPPY;

CREATE SCHEMA IF NOT EXISTS SECRETS;
USE SCHEMA POPPY.SECRETS;

CREATE OR REPLACE SECRET OUTSCRAPER_API_KEY
    TYPE = GENERIC_STRING
    SECRET_STRING = 'YXV0aDB8NjNkMzBiNDQ1NDQzYjUxNGMwMTZmMzg3fDQ4NGI1NDFkYWY';


-------------------NETWORK RULE--------------------
---------------------------------------------------
USE SCHEMA ANALYTICS;
-- Create network rule for Outscraper API
CREATE OR REPLACE NETWORK RULE outscraper_nr
 MODE = EGRESS
 TYPE = HOST_PORT
 VALUE_LIST = ('api.app.outscraper.com');

-- Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION outscraper_eai
 ALLOWED_NETWORK_RULES = (outscraper_nr)
 ALLOWED_AUTHENTICATION_SECRETS = (POPPY.SECRETS.OUTSCRAPER_API_KEY)
 ENABLED = true;

-- api_token = _snowflake.get_generic_secret_string('api_key') -- this goes into the function or called from select somehow. 

SHOW INTEGRATIONS; -- OUTSCRAPER_EAI
DESCRIBE INTEGRATION outscraper_eai;
DROP INTEGRATION <Integration_name>;
DESCRIBE INTEGRATION SNOWSERVICES_INGRESS_OAUTH; -- managed by snwoflake

---------INITIAL LOAD------------
---------------------------------

-- Create UDF to fetch initial Google reviews. This is the initial function to load all data.
CREATE OR REPLACE FUNCTION get_google_reviews(place_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
EXTERNAL_ACCESS_INTEGRATIONS = (outscraper_eai)
SECRETS = ('api_key' = POPPY.SECRETS.OUTSCRAPER_API_KEY)
HANDLER = 'get_reviews'
PACKAGES = ('requests')
AS
$$
import requests
import json
import _snowflake

def get_reviews(place_id):
    api_key = _snowflake.get_generic_secret_string('api_key')
    url = f"https://api.app.outscraper.com/maps/reviews-v3"
    params = {
        "query": place_id,
        "reviewsLimit": 1000,  
        "async": False,
        "sort":"newest"
    }
    headers = {
        "X-API-KEY": api_key
    }
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        return json.dumps(response.json())
    else:
        return json.dumps({"error": f"API request failed with status code {response.status_code}"})
$$;

SHOW FUNCTIONS LIKE '%google_reviews%';
//DROP FUNCTION GET_GOOGLE_REVIEWS(VARCHAR);

-- Test the function (replace with your place API key)
SELECT get_google_reviews('ChIJLdUfAim9hYARqRFWA5LwoAI') AS reviews;

-- Insert function results to raw data
INSERT INTO POPPY.ANALYTICS.google_reviews_json (place_id, fetch_timestamp, raw_data)
SELECT 
    'ChIJLdUfAim9hYARqRFWA5LwoAI' as place_id,
    CURRENT_TIMESTAMP() as fetch_timestamp,
    get_google_reviews('<place_id>')::VARIANT as raw_data;
    
--Test
select * from POPPY.ANALYTICS.GOOGLE_REVIEWS_JSON;

---------------Create the google_reviews_raw table. 
----This table takes in flattened fields from reviews
------------------------------------------------------

DROP TABLE GOOGLE_REVIEWS_RAW;
CREATE TABLE IF NOT EXISTS POPPY.ANALYTICS.GOOGLE_REVIEWS_RAW (
    RATING NUMBER(38,0),
	REVIEWS NUMBER(38,0),
	GOOGLE_ID VARCHAR(16777216),
	REVIEW_ID VARCHAR(16777216),
	AUTHOR_LINK VARCHAR(16777216),
    AUTHOR_TITLE VARCHAR(16777216),
	AUTHOR_ID VARCHAR(16777216),
	AUTHOR_IMAGE VARCHAR(16777216),
	AUTHOR_REVIEWS_COUNT NUMBER(38,0),
	AUTHOR_RATINGS_COUNT NUMBER(38,0),
	REVIEW_TEXT VARCHAR(16777216),
	REVIEW_IMG_URLS VARCHAR(16777216),
	REVIEW_IMG_URL VARCHAR(16777216),
    REVIEW_QUESTIONS VARCHAR(16777216),
	OWNER_ANSWER VARCHAR(16777216),
    OWNER_ANSWER_TIMESTAMP_DATETIME_UTC TIMESTAMP_NTZ(9),
	REVIEW_LINK VARCHAR(16777216),
	REVIEW_RATING NUMBER(38,0),
	REVIEW_TIMESTAMP NUMBER(38,0),
	REVIEW_DATETIME_UTC TIMESTAMP_NTZ(9),
	REVIEW_LIKES NUMBER(38,0),
	REVIEWS_ID VARCHAR(16777216)
);

-- Insert the flattened data from google_reviews_json raw_data column to the table
-- Last ROW only -------------------------
INSERT INTO POPPY.ANALYTICS.GOOGLE_REVIEWS_RAW
WITH parsed_data AS (
  SELECT PARSE_JSON(raw_data) as json_data
  FROM POPPY.ANALYTICS.GOOGLE_REVIEWS_JSON
  WHERE place_id = 'ChIJLdUfAim9hYARqRFWA5LwoAI'
  ORDER BY fetch_timestamp DESC
  LIMIT 1
)
SELECT
  json_data:data[0]:rating::NUMBER(38,0) as RATING,
  json_data:data[0]:reviews::NUMBER(38,0) as REVIEWS,
  r.value:google_id::VARCHAR as GOOGLE_ID,
  r.value:review_id::VARCHAR as REVIEW_ID,
  r.value:author_link::VARCHAR as AUTHOR_LINK,
  r.value:author_title::VARCHAR as AUTHOR_TITLE,
  r.value:author_id::VARCHAR as AUTHOR_ID,
  r.value:author_image::VARCHAR as AUTHOR_IMAGE,
  r.value:author_reviews_count::NUMBER(38,0) as AUTHOR_REVIEWS_COUNT,
  r.value:author_ratings_count::NUMBER(38,0) as AUTHOR_RATINGS_COUNT,
  r.value:review_text::VARCHAR as REVIEW_TEXT,
  r.value:review_img_urls::VARIANT as REVIEW_IMG_URLS,
  r.value:review_img_url::VARCHAR as REVIEW_IMG_URL,
  r.value:review_questions::VARIANT as REVIEW_QUESTIONS,
  r.value:owner_answer::VARCHAR as OWNER_ANSWER,
  TRY_TO_TIMESTAMP_NTZ(r.value:owner_answer_timestamp_datetime_utc::VARCHAR) as OWNER_ANSWER_DATETIME_UTC,
  r.value:review_link::VARCHAR as REVIEW_LINK,
  r.value:review_rating::NUMBER(38,0) as REVIEW_RATING,
  r.value:review_timestamp::NUMBER(38,0) as REVIEW_TIMESTAMP,
  TRY_TO_TIMESTAMP_NTZ(r.value:review_datetime_utc::VARCHAR) as REVIEW_DATETIME_UTC,
  r.value:review_likes::NUMBER(38,0) as REVIEW_LIKES,
  r.value:reviews_id::VARCHAR as REVIEWS_ID
FROM parsed_data,
LATERAL FLATTEN(input => json_data:data[0]:reviews_data) r;

--Test
-- select * from POPPY.ANALYTICS.GOOGLE_REVIEWS_RAW;
-- select * from POPPY.ANALYTICS.GOOGLE_REVIEWS_JSON;

-- Create Google_reviews_V for Analytics from Google_reviews_raw--
------------------------------------------------------------------

create or replace view POPPY.ANALYTICS.GOOGLE_REVIEWS_V(
	REVIEW_ID,
	REVIEW_DATETIME_PT,
	AUTHOR_ID,
	AUTHOR_IMAGE,
	AUTHOR_TITLE,
	AUTHOR_REVIEWS_COUNT,
	AUTHOR_RATINGS_COUNT,
	REVIEW_RATING,
	REVIEW_TEXT,
	REVIEW_IMG_URLS,
	REVIEW_IMG_URL,
	REVIEW_LIKES,
    OWNER_ANSWER,
	OWNER_ANSWER_TIMESTAMP_PT,
	PK_TOTAL_NUMBER_REVIEWS,
	PK_TOTAL_RATING
) as
SELECT
    review_id,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP_NTZ(review_datetime_utc)) AS review_datetime_pt,
    author_id,
    author_image,
    author_title,
    author_reviews_count,
    author_ratings_count,
    review_rating,
    review_text,
    review_img_urls,
    review_img_url,
    review_likes,
    owner_answer,
    CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP_NTZ(owner_answer_timestamp_datetime_utc)) AS owner_answer_timestamp_pt,
    reviews as pk_total_number_reviews,
    rating as pk_total_rating  
FROM
    analytics.google_reviews_raw
WHERE review_datetime_utc <= CURRENT_TIMESTAMP();  -- For getting latest data - the time travel option. 

-- TEST
-- SHOW TABLES IN POPPY.ANALYTICS;
-- SHOW VIEWS IN POPPY.ANALYTICS;

-- DESCRIBE TABLE POPPY.ANALYTICS.GOOGLE_REVIEWS_JSON;
-- DESCRIBE TABLE POPPY.ANALYTICS.GOOGLE_REVIEWS_V;
-- SELECT * FROM POPPY.ANALYTICS.GOOGLE_REVIEWS_V;

---------------INCREMENTAL FUNCTION------------
---- RETURNS past 30 days - RUNSWEEKLY
------------------------------------------------

-- Updated get_weekly_google_reviews function
USE SCHEMA ANALYTICS;
-- Create UDF to fetch Google reviews. This is the initial function to load all data. Basically set reviews limit to maximum number
CREATE OR REPLACE FUNCTION get_google_reviews_last_30_days(place_id STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
EXTERNAL_ACCESS_INTEGRATIONS = (outscraper_eai)
SECRETS = ('api_key' = POPPY.SECRETS.OUTSCRAPER_API_KEY)
HANDLER = 'get_reviews_last_30_days'
PACKAGES = ('requests')
AS
$$
import requests
import json
from datetime import datetime, timedelta
import _snowflake

def get_reviews_last_30_days(place_id):
    api_key = _snowflake.get_generic_secret_string('api_key')
    url = "https://api.app.outscraper.com/maps/reviews-v3"
    
    thirty_days_ago = int((datetime.now() - timedelta(days=30)).timestamp())
    
    params = {
        "query": place_id,
        "reviewsLimit": 100,
        "async": False,
        "sort": "newest",
        "cutoff": thirty_days_ago
    }
    headers = {
        "X-API-KEY": api_key
    }
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        return json.dumps(response.json())
    else:
        return json.dumps({"error": f"API request failed with status code {response.status_code}"})
$$;

//Check if created
SHOW FUNCTIONS LIKE '%google_reviews%';

-- Test the function (replace with your actual API key)
SELECT PARSE_JSON(get_google_reviews_last_30_days('ChIJLdUfAim9hYARqRFWA5LwoAI')) AS last_30_days_reviews;

//Clean up functions:
-- SHOW FUNCTIONS LIKE '%google_reviews%';
-- DROP FUNCTION GET_WEEKLY_GOOGLE_REVIEWS(VARCHAR);
-- DROP FUNCTION MYFUN();
-- SHOW USER FUNCTIONS LIKE 'MYFUN';


--------------- Insert incremental data to json----------
---------------------------------------------------------
INSERT INTO google_reviews_json (place_id, raw_data)
SELECT 
    'ChIJLdUfAim9hYARqRFWA5LwoAI' as place_id, fetch_timestamp,
    get_google_reviews_last_30_days('ChIJLdUfAim9hYARqRFWA5LwoAI')::VARIANT as raw_data;

select * from POPPY.ANALYTICS.GOOGLE_REVIEWS_JSON;

--------------- Insert incremental data to RAW-------------
---Same insert script as before only last row
-----------------------------------------------------------
INSERT INTO POPPY.ANALYTICS.GOOGLE_REVIEWS_RAW
WITH latest_data AS (
  SELECT PARSE_JSON(raw_data) as json_data, fetch_timestamp
  FROM POPPY.ANALYTICS.GOOGLE_REVIEWS_JSON
  WHERE place_id = 'ChIJLdUfAim9hYARqRFWA5LwoAI' 
  ORDER BY fetch_timestamp DESC   
  LIMIT 1
),
flattened_data AS (
  SELECT
    ld.fetch_timestamp,
    ld.json_data:data[0]:rating::NUMBER(38,0) as RATING,
    ld.json_data:data[0]:reviews::NUMBER(38,0) as REVIEWS,
    r.value:google_id::VARCHAR as GOOGLE_ID,
    r.value:review_id::VARCHAR as REVIEW_ID,
    r.value:author_link::VARCHAR as AUTHOR_LINK,
    r.value:author_title::VARCHAR as AUTHOR_TITLE,
    r.value:author_id::VARCHAR as AUTHOR_ID,
    r.value:author_image::VARCHAR as AUTHOR_IMAGE,
    r.value:author_reviews_count::NUMBER(38,0) as AUTHOR_REVIEWS_COUNT,
    r.value:author_ratings_count::NUMBER(38,0) as AUTHOR_RATINGS_COUNT,
    r.value:review_text::VARCHAR as REVIEW_TEXT,
    r.value:review_img_urls::VARIANT as REVIEW_IMG_URLS,
    r.value:review_img_url::VARCHAR as REVIEW_IMG_URL,
    r.value:review_questions::VARIANT as REVIEW_QUESTIONS,
    r.value:owner_answer::VARCHAR as OWNER_ANSWER,
    TRY_TO_TIMESTAMP_NTZ(r.value:owner_answer_timestamp_datetime_utc::VARCHAR) as OWNER_ANSWER_DATETIME_UTC,
    r.value:review_link::VARCHAR as REVIEW_LINK,
    r.value:review_rating::NUMBER(38,0) as REVIEW_RATING,
    r.value:review_timestamp::NUMBER(38,0) as REVIEW_TIMESTAMP,
    TRY_TO_TIMESTAMP_NTZ(r.value:review_datetime_utc::VARCHAR) as REVIEW_DATETIME_UTC,
    r.value:review_likes::NUMBER(38,0) as REVIEW_LIKES,
    r.value:reviews_id::VARCHAR as REVIEWS_ID
  FROM latest_data ld,
  LATERAL FLATTEN(input => ld.json_data:data[0]:reviews_data) r
)
SELECT 
  fd.RATING,
  fd.REVIEWS,
  fd.GOOGLE_ID,
  fd.REVIEW_ID,
  fd.AUTHOR_LINK,
  fd.AUTHOR_TITLE,
  fd.AUTHOR_ID,
  fd.AUTHOR_IMAGE,
  fd.AUTHOR_REVIEWS_COUNT,
  fd.AUTHOR_RATINGS_COUNT,
  fd.REVIEW_TEXT,
  fd.REVIEW_IMG_URLS,
  fd.REVIEW_IMG_URL,
  fd.REVIEW_QUESTIONS,
  fd.OWNER_ANSWER,
  fd.OWNER_ANSWER_DATETIME_UTC,
  fd.REVIEW_LINK,
  fd.REVIEW_RATING,
  fd.REVIEW_TIMESTAMP,
  fd.REVIEW_DATETIME_UTC,
  fd.REVIEW_LIKES,
  fd.REVIEWS_ID
FROM flattened_data fd
WHERE NOT EXISTS (
  SELECT 1 
  FROM POPPY.ANALYTICS.GOOGLE_REVIEWS_RAW existing
  WHERE existing.REVIEW_ID = fd.REVIEW_ID
);

-- Verify new row inserted
WITH latest_data AS (
  SELECT PARSE_JSON(raw_data) as json_data, fetch_timestamp
  FROM POPPY.ANALYTICS.GOOGLE_REVIEWS_JSON
  WHERE place_id = 'ChIJLdUfAim9hYARqRFWA5LwoAI'
  ORDER BY fetch_timestamp DESC
  LIMIT 1
),
latest_reviews AS (
  SELECT r.value:review_id::VARCHAR as REVIEW_ID
  FROM latest_data,
  LATERAL FLATTEN(input => json_data:data[0]:reviews_data) r
),
new_reviews AS (
  SELECT lr.REVIEW_ID
  FROM latest_reviews lr
  LEFT JOIN POPPY.ANALYTICS.GOOGLE_REVIEWS_RAW gr ON lr.REVIEW_ID = gr.REVIEW_ID
  WHERE gr.REVIEW_ID IS NULL
)
SELECT COUNT(*) as new_reviews_count
FROM new_reviews;

-----------------PROCEDURES AND TASK----------------------
-- 3 Procedures - 1. PULL_API_TO_JSON(), 2. INSERT_NEW_RECORDS_TO_RAW()
-- Orchestrator - 3. UPDATE_GOOGLE_REVIEWS
----------------------------------------------------------

-- Procedure to pull API data into JSON table
CREATE OR REPLACE PROCEDURE POPPY.ANALYTICS.PULL_API_TO_JSON(place_id STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  result STRING;
BEGIN
  INSERT INTO POPPY.ANALYTICS.GOOGLE_REVIEWS_JSON (place_id, fetch_timestamp, raw_data)
  SELECT 
    :place_id as place_id, 
    CURRENT_TIMESTAMP() as fetch_timestamp,
    POPPY.ANALYTICS.GET_GOOGLE_REVIEWS_LAST_30_DAYS(:place_id) as raw_data;

  result := 'API data successfully pulled and inserted into JSON table';
  RETURN result;
END;
$$;



-- Procedure to insert new records from json to RAW table
CREATE OR REPLACE PROCEDURE POPPY.ANALYTICS.INSERT_NEW_RECORDS_TO_RAW(place_id STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  result STRING;
BEGIN
  -- Insert new records from JSON to RAW table
  INSERT INTO POPPY.ANALYTICS.GOOGLE_REVIEWS_RAW
  WITH latest_json AS (
    SELECT raw_data
    FROM POPPY.ANALYTICS.GOOGLE_REVIEWS_JSON
    WHERE place_id = :place_id  
    ORDER BY fetch_timestamp DESC
    LIMIT 1
  ),
  flattened_data AS (
    SELECT
      PARSE_JSON(raw_data):data[0]:rating::NUMBER(38,0) as RATING,
      PARSE_JSON(raw_data):data[0]:reviews::NUMBER(38,0) as REVIEWS,
      r.value:google_id::VARCHAR as GOOGLE_ID,
      r.value:review_id::VARCHAR as REVIEW_ID,
      r.value:author_link::VARCHAR as AUTHOR_LINK,
      r.value:author_title::VARCHAR as AUTHOR_TITLE,
      r.value:author_id::VARCHAR as AUTHOR_ID,
      r.value:author_image::VARCHAR as AUTHOR_IMAGE,
      r.value:author_reviews_count::NUMBER(38,0) as AUTHOR_REVIEWS_COUNT,
      r.value:author_ratings_count::NUMBER(38,0) as AUTHOR_RATINGS_COUNT,
      r.value:review_text::VARCHAR as REVIEW_TEXT,
      r.value:review_img_urls::VARIANT as REVIEW_IMG_URLS,
      r.value:review_img_url::VARCHAR as REVIEW_IMG_URL,
      r.value:review_questions::VARIANT as REVIEW_QUESTIONS,
      r.value:owner_answer::VARCHAR as OWNER_ANSWER,
      TRY_TO_TIMESTAMP_NTZ(r.value:owner_answer_timestamp_datetime_utc::VARCHAR) as OWNER_ANSWER_DATETIME_UTC,
      r.value:review_link::VARCHAR as REVIEW_LINK,
      r.value:review_rating::NUMBER(38,0) as REVIEW_RATING,
      r.value:review_timestamp::NUMBER(38,0) as REVIEW_TIMESTAMP,
      TRY_TO_TIMESTAMP_NTZ(r.value:review_datetime_utc::VARCHAR) as REVIEW_DATETIME_UTC,
      r.value:review_likes::NUMBER(38,0) as REVIEW_LIKES,
      r.value:reviews_id::VARCHAR as REVIEWS_ID
    FROM latest_json,
    LATERAL FLATTEN(input => PARSE_JSON(raw_data):data[0]:reviews_data) r
  )
  SELECT fd.*
  FROM flattened_data fd
  LEFT JOIN POPPY.ANALYTICS.GOOGLE_REVIEWS_RAW raw
    ON fd.REVIEW_ID = raw.REVIEW_ID
  WHERE raw.REVIEW_ID IS NULL;
  
  result := 'New records successfully inserted into RAW table for place_id: ' || :place_id;
  RETURN result;
END;
$$;

-- Test procedure POPPY.ANALYTICS.INSERT_NEW_RECORDS_TO_RAW()
--CALL POPPY.ANALYTICS.INSERT_NEW_RECORDS_TO_RAW('ChIJLdUfAim9hYARqRFWA5LwoAI');

-- Main Orchestrator Procedure -- call both procedures. 
-- PULL_API_TO_JSON() and INSERT_NEW_RECORDS_TO_RAW()
-------------------------------------------------------

-- Main Orchestrator Procedure
CREATE OR REPLACE PROCEDURE POPPY.ANALYTICS.UPDATE_GOOGLE_REVIEWS(place_id STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  api_result STRING;
  insert_result STRING;
BEGIN
  -- Call procedure to pull API data into JSON table
  CALL POPPY.ANALYTICS.PULL_API_TO_JSON(:place_id) INTO :api_result;
  
  -- Call procedure to insert new records from JSON to RAW table
  -- If you modified INSERT_NEW_RECORDS_TO_RAW to accept place_id, use this line:
  -- CALL POPPY.ANALYTICS.INSERT_NEW_RECORDS_TO_RAW(:place_id) INTO :insert_result;
  -- Otherwise, use this line:
  CALL POPPY.ANALYTICS.INSERT_NEW_RECORDS_TO_RAW() INTO :insert_result;
  
  RETURN 'Google Reviews Update Complete for place_id: ' || :place_id || '. ' || :api_result || ' ' || :insert_result;
END;
$$;

-----SCHEDULE-------------
--------------------------

-- Create scheduled task for google reviews update - every 7 days
CREATE OR REPLACE TASK POPPY.ANALYTICS.UPDATE_GOOGLE_REVIEWS_TASK
WAREHOUSE = POPPY_WH
SCHEDULE = 'USING CRON 0 15 * * FRI America/Los_Angeles'
AS
CALL POPPY.ANALYTICS.UPDATE_GOOGLE_REVIEWS('ChIJLdUfAim9hYARqRFWA5LwoAI');

-- Activate the task
ALTER TASK POPPY.ANALYTICS.UPDATE_GOOGLE_REVIEWS_TASK RESUME;

SHOW TASKS;

---------------------END-------------------------
-------------------------------------------------

select * from POPPY.ANALYTICS.GOOGLE_REVIEWS_V;
select * from POPPY.ANALYTICS.GOOGLE_REVIEWS_JSON;

select * from POPPY.ANALYTICS.GOOGLE_REVIEWS_RAW;




