import requests
import json
import pandas as pd
import pandas_gbq
from google.cloud import secretmanager
from google.auth import default
from datetime import datetime
import time
from retry import retry
from zoneinfo import ZoneInfo 
import base64
import functions_framework

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    print(base64.b64decode(cloud_event.data["message"]["data"]))

################################ AUTHENTICATION ###############################

# Function for fetching secrets from GCP Secret Manager
def get_secret(secret_id):
    try:
        # Create the Secret Manager client
        sm_client = secretmanager.SecretManagerServiceClient()

        # Build the secret name/path
        secret_name = sm_client.secret_path('sylvan-mode-413619', secret_id) + '/versions/1'

        # Access the secret's value
        secret_string = sm_client.access_secret_version(name=secret_name).payload.data.decode('utf-8')

        try:
            # Try to load the secret as JSON
            return json.loads(secret_string)
        except json.JSONDecodeError:
            # If it fails, return the secret string as is
            return secret_string
    
    except Exception as e:
        print(f"Error occurred while fetching secret from Secret Manager: {e}")
        return None

# Define default GCP credentials and scopes
credentials, project = default(scopes=['https://www.googleapis.com/auth/bigquery',
                                       'https://www.googleapis.com/auth/cloud-platform',
                                       'https://www.googleapis.com/auth/drive'])

# Fetch TomTom Traffic API key
tomtom_api_key = get_secret('TOMTOM_API_KEY')
# Fetch OpenWeather API key
openweather_api_key = get_secret('OPENWEATHER_API_KEY')

############################## VARIABLE SETUP #################################

# Define the time zone
cet_timezone = ZoneInfo('Europe/Copenhagen')

# Get the current date and time
current_datetime = datetime.now(cet_timezone)
current_date = current_datetime.strftime('%Y-%m-%d')
current_time = current_datetime.strftime('%H:%M')

# Request URLs
traffic_request_url = 'https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/20/json?key={api_key}&point={lat},{lon}'
weather_request_url = 'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}'

############################# FUNCTION CREATION ###############################

# Function for fetching API data
@retry(tries = 3, delay = 1)
def fetch_api_data(lat, lon, geo_name, api_key, request_url, data_type):

    # Format the URL with latitude, longitude, and API key
    request_url = request_url.format(lat=lat, lon=lon, api_key=api_key)
    
    # Send request and get JSON response
    response = requests.get(request_url)
    
    # Parse the response JSON content
    if response.status_code == 200:
        data = json.loads(response.content)
        return data
    else:
        print(f"Error occured while fetching {data_type} data for {geo_name}: {response.status_code}")
        # Raise an exception to trigger retry logic
        raise Exception(f"Error occurred while fetching {data_type} data for {geo_name}: {response.status_code}")
#______________________________________________________________________________

# Function for handling traffic data and input it in a dataframe
def handle_traffic_data(data, lat, lon, geo_name, data_type):    
    try:
        # Extract fields
        road_class = data['flowSegmentData']['frc'] # Functional Road Class
        current_speed = data['flowSegmentData']['currentSpeed']
        free_flow_speed = data['flowSegmentData']['freeFlowSpeed']
        current_travel_time = data['flowSegmentData']['currentTravelTime']
        free_flow_travel_time = data['flowSegmentData']['freeFlowTravelTime']
        confidence = data['flowSegmentData']['confidence']
        road_closure = data['flowSegmentData']['roadClosure']
        first_coordinates = data['flowSegmentData']['coordinates']['coordinate'][0]
        last_coordinates = data['flowSegmentData']['coordinates']['coordinate'][-1]

        # Create dataframe and input fields
        traffic_df = pd.DataFrame({
            'date': [current_date],
            'time': [current_time],
            'geo_name': [geo_name],
            'latitude': [lat],
            'longitude': [lon],
            'road_class': [road_class],
            'current_speed': [current_speed],
            'free_flow_speed': [free_flow_speed],
            'current_travel_time': [current_travel_time],
            'free_flow_travel_time': [free_flow_travel_time],
            'confidence': [confidence],
            'road_closure': [road_closure],
            'original_coordinates': lat + ',' + lon,
            'first_coordinates': str(first_coordinates['latitude']) + ',' + str(first_coordinates['longitude']),
            'last_coordinates': str(last_coordinates['latitude']) + ',' + str(last_coordinates['longitude'])
        })
    except Exception as e:
        print(f'Error occured while handling {data_type} data for {geo_name}: {e}')
        return None
    
    return traffic_df
#______________________________________________________________________________

# Function for handling weather data and input it in a dataframe
def handle_weather_data(data, lat, lon, geo_name, data_type):
    
    # Create empty dataframe in case of error
    weather_df = pd.DataFrame()
    
    try:
        # Extract fields
        weather_main = data['weather'][0]['main']
        weather_description = data['weather'][0]['description']
        temperature = data['main']['temp'] - 273.15 # Convert Kelvin to Celcius
        feels_like = data['main']['feels_like'] - 273.15 # Convert Kelvin to Celcius
        temp_min = data['main']['temp_min'] - 273.15 # Convert Kelvin to Celcius
        temp_max = data['main']['temp_max'] - 273.15 # Convert Kelvin to Celcius
        pressure = data['main']['pressure']
        humidity_percent = data['main']['humidity']
        visibility = data['visibility']
        wind_speed = data['wind']['speed']
        wind_direction_degrees = data['wind']['deg']
        cloudiness_percent = data['clouds']['all']
        country = data['sys']['country']
        city_area_name = data['name']

        # Create dataframe and input fields
        weather_df = pd.DataFrame({
            'date': [current_date],
            'time': [current_time],
            'geo_name': [geo_name],
            'original_coordinates': lat + ',' + lon,
            'country': [country],
            'city_area_name': [city_area_name],
            'weather_main': [weather_main],
            'weather_description': [weather_description],
            'temperature': [temperature],
            'feels_like': [feels_like],
            'temp_min': [temp_min],
            'temp_max': [temp_max],
            'pressure': [pressure],
            'humidity_percent': [humidity_percent],
            'visibility': [visibility],
            'wind_speed': [wind_speed],
            'wind_direction_degrees': [wind_direction_degrees],
            'cloudiness_percent': [cloudiness_percent]
        })
    except Exception as e:
        print(f'Error occured while handling {data_type} data for {geo_name}: {e}')
        return None
    
    return weather_df
#______________________________________________________________________________

# Function to export dataframes to BigQuery
def export_to_bigquery(dataframe, table_name, data_type, geo_name):
    try:
        pandas_gbq.to_gbq(
            dataframe = dataframe,
            destination_table = 'copenhagen_data.' + table_name,
            project_id = 'sylvan-mode-413619',
            if_exists = 'append',
            credentials = credentials,
            api_method = "load_csv",
            progress_bar = False)
        
        print(f"Successfully exported {data_type} data for {geo_name}")
        
    except Exception as e:
        print(f"Error occurred while exporting {data_type} data for {geo_name} to BigQuery: {e}")
   
#______________________________________________________________________________

def fetch_handle_export(lat, lon, geo_name, api_key, request_url, data_type, table_name):
    try:
        # Fetch data from API and get JSON response
        data = fetch_api_data(lat, lon, geo_name, api_key, request_url, data_type)
        
        # Handle JSON response and input in dataframe
        if data_type == 'traffic':
            dataframe = handle_traffic_data(data, lat, lon, geo_name, data_type)
        elif data_type == 'weather':
            dataframe = handle_weather_data(data, lat, lon, geo_name, data_type)
        
        # If dataframe is None or empty, log an error and return early
        if dataframe is None or dataframe.empty:
            print(f"No {data_type} data to export for {geo_name}")
            return

        # Export dataframe to BigQuery
        export_to_bigquery(dataframe, table_name, data_type, geo_name)
        
    except Exception as e:
        print(f"Error exporting {data_type} data for {geo_name}: {e}")


######################## GEO POINT DICTIONARY CREATION ########################

geo_dict = {
    1: {'geo_name': 'bispeengbuen/aagade',
        'lat': '55.690388',
        'lon': '12.537862'},
    2: {'geo_name': 'aaboulevarden/rosenoerns alle',
        'lat': '55.681952',
        'lon': '12.557837'},
    3: {'geo_name': 'h.c. andersens boulevard/raadhuspladsen',
        'lat': '55.675732',
        'lon': '12.568113'},
    4: {'geo_name': 'amagerbrogade/vermlandsgade',
        'lat': '55.668789',
        'lon': '12.596255'},
    5: {'geo_name': 'noerrebros runddel',
        'lat': '55.694372',
        'lon': '12.548890'},
    6: {'geo_name': 'vesterbrogade/roskildevej',
        'lat': '55.670884',
        'lon': '12.531113'},
    7: {'geo_name': 'vesterbrogade/platanvej',
        'lat': '55.670272',
        'lon': '12.539123'},
    8: {'geo_name': 'kongens nytorv',
        'lat': '55.680507',
        'lon': '12.585051'},
    9: {'geo_name': 'gothersgade/adelgade',
        'lat': '55.682283',
        'lon': '12.582277'},
    10: {'geo_name': 'sydhavnsgade',
        'lat': '55.649177',
        'lon': '12.540928'},
    11: {'geo_name': 'enghavevej/vigerslev alle',
        'lat': '55.662557',
        'lon': '12.541512'},
    12: {'geo_name': 'kalvebod brygge',
        'lat': '55.666355',
        'lon': '12.567951'},
    13: {'geo_name': 'frederiksborggade/noerre farigmagsgade',
        'lat': '55.685047',
        'lon': '12.568365'},
    14: {'geo_name': 'oesterbrogade/strandboulevarden',
        'lat': '55.709179',
        'lon': '12.577500'},
    15: {'geo_name': 'lyngbyvej/rovsingsgade',
        'lat': '55.713370',
        'lon': '12.559609'},
    16: {'geo_name': 'tagensvej/jagtvej',
        'lat': '55.699442',
        'lon': '12.553815'},
    17: {'geo_name': 'vejlands alle/oerestads boulevard',
        'lat': '55.640028',
        'lon': '12.583338'},
    18: {'geo_name': 'vibenhus runddel',
        'lat': '55.706433',
        'lon': '12.562938'},
    19: {'geo_name': 'gammel koege landevej/folehaven',
        'lat': '55.650763',
        'lon': '12.507822'},
    20: {'geo_name': 'borups alle/hulgaardvej',
        'lat': '55.702012',
        'lon': '12.519450'}
    }

############################## SCRIPT EXECUTION ###############################

print(f" ----------------------------------------------------- \n ----------------------------------------------------- \n ----------------------------------------------------- \n Script execution started on {current_date} at {current_time} \n ----------------------------------------------------- \n ----------------------------------------------------- \n -----------------------------------------------------")

# Start timer
start_time = time.time()

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

# End timer
end_time = time.time()
# Calculate execution time
total_time = end_time - start_time
    
print(f" ----------------------------------------------------- \n ----------------------------------------------------- \n ----------------------------------------------------- \n Script execution finnished succesfully for {current_date} at {current_time} \n ----------------------------------------------------- \n ----------------------------------------------------- \n -----------------------------------------------------")

print(f"-----------------------------------------------------\n Total execution time: {total_time/60} minutes \n-----------------------------------------------------")




