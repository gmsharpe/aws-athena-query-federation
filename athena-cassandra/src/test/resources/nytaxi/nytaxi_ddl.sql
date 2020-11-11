---
-- #%L
-- athena-cassandra
-- %%
-- Copyright (C) 2019 - 2020 Amazon Web Services
-- %%
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--      http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- #L%
---
-- actually, 'cql', not sql, but using .sql for convenient formatting in IDE
CREATE TABLE trips (
    id UUID PRIMARY KEY,
    medallion text,
    hack_license text,
    vendor_id varchar,
    rate_code smallint,
    store_and_fwd_flag varchar,
    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    passenger_count smallint,
    trip_time_in_secs int,
    trip_distance decimal,
    pickup_longitude decimal,
    pickup_latitude decimal,
    dropoff_longitude decimal,
    dropoff_latitude decimal;

CREATE TABLE fares (
    id UUID PRIMARY KEY,
    medallion text,
    hack_license text,
    vendor_id varchar,
    pickup_datetime timestamp,
    payment_type varchar,
    fare_amount decimal,
    surcharge decimal,
    mta_tax decimal,
    tip_amount decimal,
    tolls_amount decimal,
    total_amount decimal );
