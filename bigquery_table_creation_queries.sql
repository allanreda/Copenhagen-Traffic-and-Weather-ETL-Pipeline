CREATE TABLE `sylvan-mode-413619.copenhagen_data.weather_table` (
  date STRING NOT NULL,
  time STRING NOT NULL,
  geo_name STRING NOT NULL,
  country STRING NOT NULL,
  city_area_name STRING NOT NULL,
  weather_main STRING NOT NULL,
  weather_description STRING NOT NULL,
  temperature FLOAT64 NOT NULL,
  feels_like FLOAT64 NOT NULL,
  temp_min FLOAT64 NOT NULL,
  temp_max FLOAT64 NOT NULL,
  pressure INT64 NOT NULL,
  humidity_percent INT64 NOT NULL,
  visibility INT64 NOT NULL,
  wind_speed FLOAT64 NOT NULL,
  wind_direction_degrees INT64 NOT NULL,
  cloudiness_percent INT64 NOT NULL,
  original_coordinates STRING NOT NULL
);

CREATE TABLE `sylvan-mode-413619.copenhagen_data.traffic_table` (
  date STRING NOT NULL,
  time STRING NOT NULL,
  geo_name STRING NOT NULL,
  latitude STRING NOT NULL,
  longitude STRING NOT NULL,
  road_class STRING NOT NULL,
  current_speed INT64 NOT NULL,
  free_flow_speed INT64 NOT NULL,
  current_travel_time INT64 NOT NULL,
  free_flow_travel_time INT64 NOT NULL,
  confidence FLOAT64 NOT NULL,
  road_closure BOOL NOT NULL,
  original_coordinates STRING NOT NULL,
  first_coordinates STRING NOT NULL,
  last_coordinates STRING NOT NULL
);
