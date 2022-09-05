# syntax=docker/dockerfile:experimental

# This relies on the Seafowl Nightly Docker image
# You can build your own by doing (from the root dir)
# DOCKER_BUILDKIT=1 docker build . -t splitgraph/seafowl:nightly

FROM splitgraph/seafowl:nightly

RUN \
  apt update -qq && apt install -yy wget && \
  wget https://www.cl.cam.ac.uk/research/dtg/weather/weather-raw.csv && \
  seafowl --one-off \
  "CREATE EXTERNAL TABLE cambridge_weather (\
  timestamp TIMESTAMP,\
  temp_c_10 INT NULL,\
  humidity_perc INT NULL,\
  dew_point_c_10 INT NULL,\
  pressure_mbar INT NULL,\
  mean_wind_speed_knots_10 INT NULL,\
  average_wind_bearing_degrees INT NULL,\
  sunshine_hours_100 INT,\
  rainfall_mm_1000 INT,\
  max_wind_speed_knots_10 INT NULL)\
  STORED AS CSV\
  LOCATION './weather-raw.csv';\
  CREATE TABLE cambridge_weather AS SELECT * FROM staging.cambridge_weather" && \
  rm weather-raw.csv
