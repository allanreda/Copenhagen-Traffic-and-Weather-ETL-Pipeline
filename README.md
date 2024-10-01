Try the dashboard out live at: https://lookerstudio.google.com/reporting/f3e0f840-5cb6-413d-92c9-f911df595063  

![image](https://github.com/user-attachments/assets/af48ad15-a20a-4549-bf01-6709252c5df4)

## Introduction
Initially, I wanted to gather traffic and weather data for the city of Copenhagen, to see if I could create an ML-model that could predict the traffic partially based on weather conditions. I quickly realized that there was nowhere I could get that historical data to train my models - at least not for free. Since this project was only meant as a hobby project, I had no intention of paying for any API subscriptions to get the data I needed. In my search, I found out that the TomTom and OpenWeather APIs offered free options, but only for fetching current traffic and weather conditions, and not historical. That's when I decided to build an ETL pipeline to generate my own datasets over time, and create a dashboard (because why not?) to visualize real-time data - which is a full project in itself. That is the project you're going to read about here.

## Goal
The main goal of this project is to create an ETL pipeline that will fetch hourly traffic and weather data for different geographical points in Copenhagen and store them in separate BigQuery tables. The geographical points chosen for this project has to represent the most busy traffic junctions in Copenhagen, as they would theoritically be most vulnerable to traffic jams caused by weather conditions. It is a must that the pipeline is fully automated, and doesn't require any intervention at any given point.
The secondary goal of this project is to create a dashboard that visualizes the latest data, which gives a quick, almost real-time, overview of the traffic and weather conditions in Copenhagen.

## Table of Contents
- [Introduction](#introduction)
- [Goal](#goal)
- [Project Diagram](#project-diagram)
- [Cloud Run Function](#cloud-run-function)
  - [Functions](#functions)
    - [hello_pubsub](#hello_pubsub)
    - [get_secret](#get_secret)
    - [fetch_api_data](#fetch_api_data)
    - [handle_traffic_data / handle_weather_data](#handle_traffic_data--handle_weather_data)
    - [export_to_bigquery](#export_to_bigquery)
    - [fetch_handle_export](#fetch_handle_export)
  - [geo_dict and execution for loop](#geo_dict-and-execution-for-loop)
  - [Scheduling](#scheduling)
- [Scheduled Query (BigQuery)](#scheduled-query-bigquery)
- [Technologies](#technologies)

## Project Diagram
![copenhagen_data drawio (1)](https://github.com/user-attachments/assets/16fd00aa-1b7c-47d2-bda4-67bc2a6426ec)

## Cloud Run Function

### Functions  
The Python script deployed in Cloud Run Functions mainly consists of seven functions which each has their own important functionality.  
They are as follows:  

#### hello_pubsub
Receives the message from Pub/Sub and gets triggered. This invokes the script.

#### get_secret
Fetches API keys for the TomTom and OpenWeather APIs from Secret Manager.

#### fetch_api_data
As the name suggests, this function is responsible for fetching the raw data from both APIs. It parses the JSON response and returns it as a dictionary.

#### handle_traffic_data / handle_weather_data
These two functions extracts the needed fields from the dictionaries returned from the fetch_api_data function. They are each designed to extract data from either TomTom or OpenWeather API responses and convert them to Pandas DataFrames.

#### export_to_bigquery
Utilizes the to_gbq function from the pandas_gbq library to export the DataFrames returned from the handle_weather_data and handle_traffic_data to specific BigQuery tables.

#### fetch_handle_export
This is the main function that utilizes the fetch_api_data, handle_traffic_data or handle_weather_data (depending on the data type), and export_to_bigquery functions, basically functionalizing the pipeline.

### geo_dict and execution for loop
The geographical points chosen for this project represents the most busy traffical junctions in Copenhagen.
These points are all hardcoded and stored into the geo_dict dictionary.
Each item in the dictionary contains a street name, a latitude and a longitude. 

The actual execution and looping through the geo_dict, happens in this for loop:  
```python
# Loop through each geographical point in geo_dict
for key, value in geo_dict.items():
    # Fetch and export weather data
    try:
        fetch_handle_export(value['lat'], value['lon'], value['geo_name'], openweather_api_key, weather_request_url, 'weather', 'weather_table')
    except Exception as e:
        print(f"Error exporting weather data for {value['geo_name']}: {e}")
    
    # Fetch and export traffic data
    try:
        fetch_handle_export(value['lat'], value['lon'], value['geo_name'], tomtom_api_key, traffic_request_url, 'traffic', 'traffic_table')
    except Exception as e:
        print(f"Error exporting traffic data for {value['geo_name']}: {e}")
```

### Scheduling
The Cloud Run Function is scheduled to run hourly. This scheduling happens with the help of a Cloud Scheduler that triggers a Pub/Sub topic to send a message to the Cloud Run Function.
That message is recieved by the hello_pubsub function which activates the whole script.

## Scheduled Query (BigQuery)
Every hour, five minutes after the Cloud Run Function has been activated, the scheduled query in BigQuery is set to run. Its job is to find and join the latest rows from "traffic_table" and "weather_table" and write the result to the "latest_joined_data" table. The scheduled query is configured to overwrite the existing data in the "latest_joined_data" table, rather than append to it. This ensures that the Looker dashboard, which uses this table as its only data source, processes only the most recent data, which significantly improves performance by avoiding to filter through all records from the two source tables.  
```sql
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
```

## Technologies
This project is built using:
- Python: Building the script used in the Cloud Run Function
- SQL: Creating and joining tables in BigQuery
- TomTom API: Fetching current traffic conditions
- OpenWeather API: Fetching current weather conditions
- Google Cloud Run Function: Deployment and automation of Python script
- Google Cloud Scheduler and Pub/Sub: Scheduling and activation of Cloud Run Function
- Google Cloud Secret Manager: Secure storage and management of API keys
- BigQuery: Data storage and query scheduling
- Looker Studio: Data visualization 
