SELECT * 
FROM 
  `sylvan-mode-413619.copenhagen_data.traffic_table` AS traffic
INNER JOIN 
  `sylvan-mode-413619.copenhagen_data.weather_table` AS weather
USING (date, time, geo_name, original_coordinates)
WHERE
  DATETIME(
    PARSE_DATE('%Y-%m-%d', date),
    PARSE_TIME('%H:%M', time)
  ) = (
    SELECT MAX(DATETIME(
      PARSE_DATE('%Y-%m-%d', date),
      PARSE_TIME('%H:%M', time)
    ))
    FROM `sylvan-mode-413619.copenhagen_data.traffic_table`
  )
