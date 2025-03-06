create or replace TABLE DEMO_GEOSPATIAL.SILVER.HERE_JSON_TBLS (
	ACCESS_LAT FLOAT,
	ACCESS_LNG FLOAT,
	ADDRESS_HOUSE_NUMBER VARCHAR(16777216),
	ADDRESS_STREET VARCHAR(16777216),
	ADDRESS_CITY VARCHAR(16777216),
	ADDRESS_STATE VARCHAR(16777216),
	ADDRESS_STATE_CODE VARCHAR(16777216),
	ADDRESS_POSTALCODE VARCHAR(16777216),
	ADDRESS_COUNTY VARCHAR(16777216),
	ADDRESS_DESCRIPTION VARCHAR(16777216)
);


# ------------------------------------------------------------------------------------------------------------------------------------


# Import python packages
import streamlit as st
import pandas as pd
import _snowflake
import requests
import json

# We can also use Snowpark for our analyses!
from snowflake.snowpark.context import get_active_session
session = get_active_session()
get_active_session()

# ------------------------------------------------------------------------------------------------------------------------------------


use database DEMO_GEOSPATIAL;
use schema silver;
use role accountadmin;


 --ALLOW ACCESS TO WEBSITE TO QUERY
CREATE OR REPLACE NETWORK RULE here_gis_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('geocode.search.hereapi.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION here_gis_access_integration
  ALLOWED_NETWORK_RULES = (here_gis_rule)
  ENABLED = true;

# ------------------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE FUNCTION get_url(url string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'get_url'
EXTERNAL_ACCESS_INTEGRATIONS = (here_gis_access_integration)
PACKAGES = ('snowflake-snowpark-python','requests')
AS
$$
import _snowflake
import requests
import json
session = requests.Session()
def get_url(url):

    response = session.get(url)
    return response.text
$$;


# ------------------------------------------------------------------------------------------------------------------------------------

# random testing 
create or replace table here_json as 
select parse_json(get_url('https://geocode.search.hereapi.com/v1/geocode?q=Invalidenstr+117+Berlin&apiKey=<replace with api key>'))::variant data;


insert into here_json
select parse_json(get_url('https://geocode.search.hereapi.com/v1/geocode?q=103+Autumn Rd+Greer+SC&apiKey=<replace with api key>'))::variant data;

select get_url('https://geocode.search.hereapi.com/v1/geocode?q=Autumn+Greer+103&apiKey=<replace with api key>')

# ------------------------------------------------------------------------------------------------------------------------------------

# flatten
SELECT
    f.value:access[0].lat::float AS access_lat,
    f.value:access[0].lng::float AS access_lng,
    f.value:address.houseNumber::STRING AS address_house_number,
    f.value:address.street::STRING AS address_street,
    f.value:address.city::STRING AS address_city,
    f.value:address.state::STRING AS address_state,
    f.value:address.stateCode::STRING AS address_state_code,
    f.value:address.postalCode::STRING AS address_postalcode,
    f.value:address.county::STRING AS address_county,
FROM
    here_json, LATERAL FLATTEN(input => data:items) f;


# ------------------------------------------------------------------------------------------------------------------------------------

insert into here_json_tbls (access_lat, access_lng, address_house_number, address_street, address_city, address_state, address_state_code, address_postalcode, address_county)
with parsed_json as (select parse_json(get_url('https://geocode.search.hereapi.com/v1/geocode?q=Autumn+Rd+Greer+103&apiKey=<replace with api key>'))::variant data)
SELECT
    f.value:access[0].lat::float AS access_lat,
    f.value:access[0].lng::float AS access_lng,
    f.value:address.houseNumber::STRING AS address_house_number,
    f.value:address.street::STRING AS address_street,
    f.value:address.city::STRING AS address_city,
    f.value:address.state::STRING AS address_state,
    f.value:address.stateCode::STRING AS address_state_code,
    f.value:address.postalCode::STRING AS address_postalcode,
    f.value:address.county::STRING AS address_county,
FROM
    parsed_json, LATERAL FLATTEN(input => data:items) f;

