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
  temp_c_10 INTEGER NULL,\
  humidity_perc INTEGER NULL,\
  dew_point_c_10 INTEGER NULL,\
  pressure_mbar INTEGER NULL,\
  mean_wind_speed_knots_10 INTEGER NULL,\
  average_wind_bearing_degrees INTEGER NULL,\
  sunshine_hours_100 INTEGER,\
  rainfall_mm_1000 INTEGER,\
  max_wind_speed_knots_10 INTEGER NULL)\
  STORED AS CSV\
  LOCATION './weather-raw.csv';\
  CREATE TABLE cambridge_weather AS SELECT * FROM staging.cambridge_weather" && \
  rm weather-raw.csv
