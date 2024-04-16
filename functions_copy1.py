import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry


def filter_by_region(data_frame, region_name):
    """
    Filters a DataFrame based on the specified region name.
    
    """
    filtered_df = data_frame[data_frame['Region of Incident'] == region_name]
    return filtered_df

def get_weather(latitude, longitude, start_date, end_date):
    
    """
    Retrieves historical weather data for a specific location within a given time range.

    """
    import requests_cache
    import openmeteo_requests
    import requests_cache
    import pandas as pd
    from retry_requests import retry

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
	"latitude": latitude,
	"longitude": longitude,
	"start_date": start_date,
	"end_date": end_date,
    "daily": [
        "weather_code",
        "temperature_2m_max",
        "temperature_2m_min",
        "temperature_2m_mean",
        "sunrise",
        "sunset",
        "daylight_duration",
        "precipitation_sum",
        "wind_speed_10m_max",
        "wind_gusts_10m_max",
        "wind_direction_10m_dominant"
    ]
}
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    daily = response.Daily()
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    daily_temperature_2m_mean = daily.Variables(3).ValuesAsNumpy()
    daily_sunrise = daily.Variables(4).ValuesAsNumpy()
    daily_sunset = daily.Variables(5).ValuesAsNumpy()
    daily_daylight_duration = daily.Variables(6).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(7).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(8).ValuesAsNumpy()
    daily_wind_gusts_10m_max = daily.Variables(9).ValuesAsNumpy()
    daily_wind_direction_10m_dominant = daily.Variables(10).ValuesAsNumpy()
    
    daily_data = {"date": pd.date_range(
	start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
	end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = daily.Interval()),
	inclusive = "left"
)}
    daily_data["weather_code"] = daily_weather_code
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["sunrise"] = daily_sunrise
    daily_data["sunset"] = daily_sunset
    daily_data["daylight_duration"] = daily_daylight_duration
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max
    daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant

    daily_df = pd.DataFrame(data = daily_data)
    return daily_df
    
def split_coordinates(df, column_name):
    """
    Splits the coordinates column into latitude and longitude columns.
    
    """
    df[['latitude', 'longitude']] = df[column_name].str.split(', ', expand=True)
    
    return df

def get_weather_for_row(row):
    """
    Retrieves weather data for a specific row in a dataset.
    
    """
    latitude = row["latitude"]
    longitude = row["longitude"]
    incident_date = row["Incident Date"]
    start_date = incident_date
    end_date = incident_date
    weather_data = get_weather(latitude, longitude, start_date, end_date)
    return weather_data

def reset_and_drop_index(df):
    """
    Resets the index of a DataFrame and drops the old index.

    """
    return df.reset_index(drop=True)

def fetch_weather_data(mediterranean_df):
    """
    Fetches weather data for each row in the given DataFrame.
    """
    weather_data_dict = {}
    # Iterate over each row in the DataFrame
    for index, row in mediterranean_df.iterrows():
        result = get_weather_for_row(row)  # Assuming get_weather_for_row is defined elsewhere
        weather_data_dict[index] = result
    return weather_data_dict
    
def merge_weather_data(mediterranean_df, weather_data_dict):
    """
    Merges weather data from the dictionary into the given DataFrame.
    """
    mediterranean_df = mediterranean_df.reset_index(drop=True)
    combined_df = pd.concat([pd.DataFrame(weather_data_dict[key]) for key in weather_data_dict], ignore_index=True)
    mediterranean_df = pd.concat([mediterranean_df, combined_df], axis=1)
    return mediterranean_df
    
def initialise_df_and_add_weather(df):
    """
    Isolate data from the Mediterranean region and enrich it with weather data.
    """
    mediterranean_df = filter_by_region(df, "Mediterranean")
    split_coordinates(mediterranean_df, "Coordinates")
    reset_and_drop_index(mediterranean_df)
    weather_data_dict = fetch_weather_data(mediterranean_df)
    mediterranean_df = merge_weather_data(mediterranean_df, weather_data_dict)
    return mediterranean_df
    
def format_column_names(mdf):
    """
    Format the column names of a DataFrame by replacing spaces with underscores and converting to lowercase.
    """
    mdf.columns = mdf.columns.str.replace(' ', '_').str.lower()
    return mdf
    
def combine_and_replace_ids(mdf):
    """
    Combine and replace IDs in the DataFrame.
    """
    if 'main_id' in mdf.columns and 'incident_id' in mdf.columns:
        mdf.drop(['main_id', 'incident_id'], axis=1, inplace=True)
    else:
        print("Columns 'main_id' and/or 'incident_id' not found in the DataFrame.")
    mdf['id'] = range(1, len(mdf) + 1)
    return mdf
    
def deal_with_nullvalues(mdf):
    """
    Fills missing values in specific columns of the DataFrame `mediterranean_df`.
    """
    columns_to_fill = ["number_of_dead", "minimum_estimated_number_of_missing", "country_of_origin",
                       "information_source", "url"]
    mdf[columns_to_fill] = mdf[columns_to_fill].fillna('Unknown')
    columns_to_fill2 = ["number_of_survivors", "number_of_females",
                       "number_of_males", "number_of_children"]
    mdf[columns_to_fill2] = mdf[columns_to_fill2].fillna(0)
    mdf["url"].fillna('None', inplace=True)
    return mdf
    
def check_duplicates(mdf):
    """
    This function checks for duplicate rows in a pandas DataFrame.
    """
    duplicates = mdf[mdf.duplicated()]
    mdf = mdf.drop_duplicates()
    return mdf
    
def clean_dataframe(mdf):
    """
    Function to clean the provided dataframe by performing several data cleaning operations.
    """
    format_column_names(mdf)
    combine_and_replace_ids(mdf)
    deal_with_nullvalues(mdf)
    check_duplicates(mdf)
    return mdf