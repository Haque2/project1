{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "4f217200-cb8a-4eb9-8097-4ef3e1c2c1f1",
   "metadata": {},
   "source": [
    "# **Functions**"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "42d64955-8a67-4b2b-8e77-b92d21bb5c3b",
   "metadata": {},
   "source": [
    "## **Initalise**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "a56ea6ba-f187-44ea-8435-a0f4f0940cd5",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Requirement already satisfied: openmeteo-requests in /opt/anaconda3/lib/python3.11/site-packages (1.2.0)\n",
      "Requirement already satisfied: openmeteo-sdk>=1.4.0 in /opt/anaconda3/lib/python3.11/site-packages (from openmeteo-requests) (1.11.4)\n",
      "Requirement already satisfied: requests in /opt/anaconda3/lib/python3.11/site-packages (from openmeteo-requests) (2.31.0)\n",
      "Requirement already satisfied: flatbuffers>=24.0.0 in /opt/anaconda3/lib/python3.11/site-packages (from openmeteo-sdk>=1.4.0->openmeteo-requests) (24.3.25)\n",
      "Requirement already satisfied: charset-normalizer<4,>=2 in /opt/anaconda3/lib/python3.11/site-packages (from requests->openmeteo-requests) (2.0.4)\n",
      "Requirement already satisfied: idna<4,>=2.5 in /opt/anaconda3/lib/python3.11/site-packages (from requests->openmeteo-requests) (3.4)\n",
      "Requirement already satisfied: urllib3<3,>=1.21.1 in /opt/anaconda3/lib/python3.11/site-packages (from requests->openmeteo-requests) (2.0.7)\n",
      "Requirement already satisfied: certifi>=2017.4.17 in /opt/anaconda3/lib/python3.11/site-packages (from requests->openmeteo-requests) (2024.2.2)\n",
      "Requirement already satisfied: requests-cache in /opt/anaconda3/lib/python3.11/site-packages (1.2.0)\n",
      "Requirement already satisfied: retry-requests in /opt/anaconda3/lib/python3.11/site-packages (2.0.0)\n",
      "Requirement already satisfied: numpy in /opt/anaconda3/lib/python3.11/site-packages (1.26.4)\n",
      "Requirement already satisfied: pandas in /opt/anaconda3/lib/python3.11/site-packages (2.1.4)\n",
      "Requirement already satisfied: attrs>=21.2 in /opt/anaconda3/lib/python3.11/site-packages (from requests-cache) (23.1.0)\n",
      "Requirement already satisfied: cattrs>=22.2 in /opt/anaconda3/lib/python3.11/site-packages (from requests-cache) (23.2.3)\n",
      "Requirement already satisfied: platformdirs>=2.5 in /opt/anaconda3/lib/python3.11/site-packages (from requests-cache) (3.10.0)\n",
      "Requirement already satisfied: requests>=2.22 in /opt/anaconda3/lib/python3.11/site-packages (from requests-cache) (2.31.0)\n",
      "Requirement already satisfied: url-normalize>=1.4 in /opt/anaconda3/lib/python3.11/site-packages (from requests-cache) (1.4.3)\n",
      "Requirement already satisfied: urllib3>=1.25.5 in /opt/anaconda3/lib/python3.11/site-packages (from requests-cache) (2.0.7)\n",
      "Requirement already satisfied: python-dateutil>=2.8.2 in /opt/anaconda3/lib/python3.11/site-packages (from pandas) (2.8.2)\n",
      "Requirement already satisfied: pytz>=2020.1 in /opt/anaconda3/lib/python3.11/site-packages (from pandas) (2023.3.post1)\n",
      "Requirement already satisfied: tzdata>=2022.1 in /opt/anaconda3/lib/python3.11/site-packages (from pandas) (2023.3)\n",
      "Requirement already satisfied: six>=1.5 in /opt/anaconda3/lib/python3.11/site-packages (from python-dateutil>=2.8.2->pandas) (1.16.0)\n",
      "Requirement already satisfied: charset-normalizer<4,>=2 in /opt/anaconda3/lib/python3.11/site-packages (from requests>=2.22->requests-cache) (2.0.4)\n",
      "Requirement already satisfied: idna<4,>=2.5 in /opt/anaconda3/lib/python3.11/site-packages (from requests>=2.22->requests-cache) (3.4)\n",
      "Requirement already satisfied: certifi>=2017.4.17 in /opt/anaconda3/lib/python3.11/site-packages (from requests>=2.22->requests-cache) (2024.2.2)\n"
     ]
    }
   ],
   "source": [
    "!pip install openmeteo-requests\n",
    "!pip install requests-cache retry-requests numpy pandas\n",
    "import openmeteo_requests\n",
    "import requests_cache\n",
    "import pandas as pd\n",
    "from retry_requests import retry"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "030c7953-d36f-4821-adb4-3a41a45c5647",
   "metadata": {},
   "source": [
    "## **Collect Data Functions**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "236fe843-8d6d-4f23-aea0-027be59fc68c",
   "metadata": {},
   "outputs": [],
   "source": [
    "#mediterranean_df = df[df['Region of Incident'] == 'Mediterranean']\n",
    "def filter_by_region(data_frame, region_name):\n",
    "    \"\"\"\n",
    "    Filters a DataFrame based on the specified region name.\n",
    "    \n",
    "    \"\"\"\n",
    "    filtered_df = data_frame[data_frame['Region of Incident'] == region_name]\n",
    "    return filtered_df\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "0bca0bcd-1e82-443a-a04f-38964458b04e",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Setup the Open-Meteo API client with cache and retry on error\n",
    "cache_session = requests_cache.CachedSession('.cache', expire_after = -1)\n",
    "retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)\n",
    "openmeteo = openmeteo_requests.Client(session = retry_session)\n",
    "\n",
    "def get_weather(latitude, longitude, start_date, end_date):\n",
    "    \n",
    "    \"\"\"\n",
    "    Retrieves historical weather data for a specific location within a given time range.\n",
    "\n",
    "    \"\"\"\n",
    "    url = \"https://archive-api.open-meteo.com/v1/archive\"\n",
    "    params = {\n",
    "\t\"latitude\": latitude,\n",
    "\t\"longitude\": longitude,\n",
    "\t\"start_date\": start_date,\n",
    "\t\"end_date\": end_date,\n",
    "    \"daily\": [\n",
    "        \"weather_code\",\n",
    "        \"temperature_2m_max\",\n",
    "        \"temperature_2m_min\",\n",
    "        \"temperature_2m_mean\",\n",
    "        \"sunrise\",\n",
    "        \"sunset\",\n",
    "        \"daylight_duration\",\n",
    "        \"precipitation_sum\",\n",
    "        \"wind_speed_10m_max\",\n",
    "        \"wind_gusts_10m_max\",\n",
    "        \"wind_direction_10m_dominant\"\n",
    "    ]\n",
    "}\n",
    "    responses = openmeteo.weather_api(url, params=params)\n",
    "    response = responses[0]\n",
    "    daily = response.Daily()\n",
    "    daily_weather_code = daily.Variables(0).ValuesAsNumpy()\n",
    "    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()\n",
    "    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()\n",
    "    daily_temperature_2m_mean = daily.Variables(3).ValuesAsNumpy()\n",
    "    daily_sunrise = daily.Variables(4).ValuesAsNumpy()\n",
    "    daily_sunset = daily.Variables(5).ValuesAsNumpy()\n",
    "    daily_daylight_duration = daily.Variables(6).ValuesAsNumpy()\n",
    "    daily_precipitation_sum = daily.Variables(7).ValuesAsNumpy()\n",
    "    daily_wind_speed_10m_max = daily.Variables(8).ValuesAsNumpy()\n",
    "    daily_wind_gusts_10m_max = daily.Variables(9).ValuesAsNumpy()\n",
    "    daily_wind_direction_10m_dominant = daily.Variables(10).ValuesAsNumpy()\n",
    "    \n",
    "    daily_data = {\"date\": pd.date_range(\n",
    "\tstart = pd.to_datetime(daily.Time(), unit = \"s\", utc = True),\n",
    "\tend = pd.to_datetime(daily.TimeEnd(), unit = \"s\", utc = True),\n",
    "\tfreq = pd.Timedelta(seconds = daily.Interval()),\n",
    "\tinclusive = \"left\"\n",
    ")}\n",
    "    daily_data[\"weather_code\"] = daily_weather_code\n",
    "    daily_data[\"temperature_2m_max\"] = daily_temperature_2m_max\n",
    "    daily_data[\"temperature_2m_min\"] = daily_temperature_2m_min\n",
    "    daily_data[\"temperature_2m_mean\"] = daily_temperature_2m_mean\n",
    "    daily_data[\"sunrise\"] = daily_sunrise\n",
    "    daily_data[\"sunset\"] = daily_sunset\n",
    "    daily_data[\"daylight_duration\"] = daily_daylight_duration\n",
    "    daily_data[\"precipitation_sum\"] = daily_precipitation_sum\n",
    "    daily_data[\"wind_speed_10m_max\"] = daily_wind_speed_10m_max\n",
    "    daily_data[\"wind_gusts_10m_max\"] = daily_wind_gusts_10m_max\n",
    "    daily_data[\"wind_direction_10m_dominant\"] = daily_wind_direction_10m_dominant\n",
    "\n",
    "    daily_df = pd.DataFrame(data = daily_data)\n",
    "    return daily_df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "6ae32169-705d-4300-9386-b778e304679d",
   "metadata": {},
   "outputs": [],
   "source": [
    "#mediterranean_df[['latitude', 'longitude']] = mediterranean_df['Coordinates'].str.split(', ', expand=True)\n",
    "\n",
    "def split_coordinates(df, column_name):\n",
    "    \"\"\"\n",
    "    Splits the coordinates column into latitude and longitude columns.\n",
    "    \n",
    "    \"\"\"\n",
    "    df[['latitude', 'longitude']] = df[column_name].str.split(', ', expand=True)\n",
    "    \n",
    "    return df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "e58bf2a5-0181-469c-b09d-594055283092",
   "metadata": {},
   "outputs": [],
   "source": [
    "def get_weather_for_row(row):\n",
    "    \"\"\"\n",
    "    Retrieves weather data for a specific row in a dataset.\n",
    "    \n",
    "    \"\"\"\n",
    "    latitude = row[\"latitude\"]\n",
    "    longitude = row[\"longitude\"]\n",
    "    incident_date = row[\"Incident Date\"]\n",
    "    start_date = incident_date\n",
    "    end_date = incident_date\n",
    "    weather_data = get_weather(latitude, longitude, start_date, end_date)\n",
    "    return weather_data"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "97fbdcb0-1edc-427f-b364-a7dfb2e79dc4",
   "metadata": {},
   "outputs": [],
   "source": [
    "def reset_and_drop_index(df):\n",
    "    \"\"\"\n",
    "    Resets the index of a DataFrame and drops the old index.\n",
    "\n",
    "    \"\"\"\n",
    "    return df.reset_index(drop=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "2d0d94ab-cf9b-4b42-aa5a-0fe49bd049db",
   "metadata": {},
   "outputs": [],
   "source": [
    "def fetch_weather_data(mediterranean_df):\n",
    "    \"\"\"\n",
    "    Fetches weather data for each row in the given DataFrame.\n",
    "\n",
    "    \"\"\"\n",
    "    weather_data_dict = {}\n",
    "    \n",
    "    # Iterate over each row in the DataFrame\n",
    "    for index, row in mediterranean_df.iterrows():\n",
    "        result = get_weather_for_row(row)  # Assuming get_weather_for_row is defined elsewhere\n",
    "        weather_data_dict[index] = result\n",
    "    \n",
    "    return weather_data_dict\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "45744bfa-cd58-4fc0-b7b9-7d1cdf2837d6",
   "metadata": {},
   "outputs": [],
   "source": [
    "def merge_weather_data(mediterranean_df, weather_data_dict):\n",
    "    \"\"\"\n",
    "    Merges weather data from the dictionary into the given DataFrame.\n",
    "\n",
    "    \"\"\"\n",
    "    mediterranean_df = mediterranean_df.reset_index(drop=True)\n",
    "    combined_df = pd.concat([pd.DataFrame(weather_data_dict[key]) for key in weather_data_dict], ignore_index=True)\n",
    "    mediterranean_df = pd.concat([mediterranean_df, combined_df], axis=1)\n",
    "    \n",
    "    return mediterranean_df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "32d489c5-0630-4cac-9c96-cebc0aa57379",
   "metadata": {},
   "outputs": [],
   "source": [
    "def initialise_df_and_add_weather(df):\n",
    "\n",
    "    \"\"\"\n",
    "    Isolate data from the Mediterranean region and enrich it with weather data.\n",
    "\n",
    "    \"\"\"\n",
    "    mediterranean_df = filter_by_region(df, \"Mediterranean\")\n",
    "    split_coordinates(mediterranean_df, \"Coordinates\")\n",
    "    reset_and_drop_index(mediterranean_df)\n",
    "    weather_data_dict = fetch_weather_data(mediterranean_df)\n",
    "    mediterranean_df = merge_weather_data(mediterranean_df, weather_data_dict)\n",
    "    return mediterranean_df"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "9df2bd91-e757-4543-89a2-f39734b2c979",
   "metadata": {},
   "source": [
    "# **TEST**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "ea90ee7b-069f-4f44-b4ed-5c894c47720f",
   "metadata": {},
   "outputs": [],
   "source": [
    "pd.set_option('display.max_columns', None)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "id": "62ab1de4-f503-46e0-b128-6863e58869ce",
   "metadata": {},
   "outputs": [],
   "source": [
    "df = pd.read_csv('Missing_Migrants_Global_Figures_allData.csv')"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "784f74a3-624d-4093-b459-725f0680803c",
   "metadata": {},
   "source": [
    "## **Cleaning Functions**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "77453d18-2ac7-4a81-a384-659b9728b9de",
   "metadata": {},
   "outputs": [],
   "source": [
    "def format_column_names(mdf):\n",
    "    \"\"\"\n",
    "    Format the column names of a DataFrame by replacing spaces with underscores and converting to lowercase.\n",
    "\n",
    "    \"\"\"\n",
    "    mdf.columns = mdf.columns.str.replace(' ', '_').str.lower()\n",
    "    return mdf"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "id": "6fb97560-fd3c-4311-a886-a9dafc490a05",
   "metadata": {},
   "outputs": [],
   "source": [
    "def combine_and_replace_ids(mdf):\n",
    "    \"\"\"\n",
    "    Combine and replace IDs in the DataFrame.\n",
    "    \n",
    "    \"\"\"\n",
    "    if 'main_id' in mdf.columns and 'incident_id' in mdf.columns:\n",
    "        mdf.drop(['main_id', 'incident_id'], axis=1, inplace=True)\n",
    "    else:\n",
    "        print(\"Columns 'main_id' and/or 'incident_id' not found in the DataFrame.\")\n",
    "    mdf['id'] = range(1, len(mdf) + 1)\n",
    "    return mdf"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "id": "369b6530-4f14-471c-8e8c-4530d2263646",
   "metadata": {},
   "outputs": [],
   "source": [
    "def deal_with_nullvalues(mdf):\n",
    "    \"\"\"\n",
    "    Fills missing values in specific columns of the DataFrame `mediterranean_df`.\n",
    "\n",
    "    \"\"\"\n",
    "    columns_to_fill = [\"number_of_dead\", \"minimum_estimated_number_of_missing\", \"country_of_origin\", \n",
    "                       \"information_source\", \"url\", \"number_of_survivors\", \"number_of_females\", \n",
    "                       \"number_of_males\", \"number_of_children\"]\n",
    "    \n",
    "    mdf[columns_to_fill] = mdf[columns_to_fill].fillna('Unknown')\n",
    "    mdf[\"url\"].fillna('None', inplace=True)  \n",
    "    \n",
    "    return mdf"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "id": "cc2f3724-1c41-4a56-b829-76b370b33afe",
   "metadata": {},
   "outputs": [],
   "source": [
    "def check_duplicates(mdf):\n",
    "    \"\"\"\n",
    "    This function checks for duplicate rows in a pandas DataFrame.\n",
    "    \n",
    "    \"\"\"\n",
    "    duplicates = mdf[mdf.duplicated()]\n",
    "    mdf = mdf.drop_duplicates()\n",
    "    return mdf"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 21,
   "id": "1646d170-c687-4de4-8f48-8a26099a5748",
   "metadata": {},
   "outputs": [],
   "source": [
    "def clean_dataframe(mdf):\n",
    "    \"\"\"\n",
    "    Function to clean the provided dataframe by performing several data cleaning operations.\n",
    "    \n",
    "    \"\"\"\n",
    "    format_column_names(mdf)\n",
    "    combine_and_replace_ids(mdf)\n",
    "    deal_with_nullvalues(mdf)\n",
    "    check_duplicates(mdf)\n",
    "    return mdf"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.7"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
