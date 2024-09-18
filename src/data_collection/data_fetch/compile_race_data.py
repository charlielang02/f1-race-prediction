import os
import pickle
import requests
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import time

CACHE_DIR = 'cache'
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def cache_get(cache_key):
    cache_path = os.path.join(CACHE_DIR, cache_key)
    if os.path.exists(cache_path):
        with open(cache_path, 'rb') as cache_file:
            return pickle.load(cache_file)
    return None

def cache_set(cache_key, data):
    cache_path = os.path.join(CACHE_DIR, cache_key)
    with open(cache_path, 'wb') as cache_file:
        pickle.dump(data, cache_file)

def fetch_single_race(year, round_num, max_retries=3):
    url = f'http://ergast.com/api/f1/{year}/{round_num}/results.json'
    retries = 0
    backoff = 1

    while retries < max_retries:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                race_data = response.json()['MRData']['RaceTable']['Races']
                return race_data
        except requests.exceptions.Timeout:
            print(f"Timeout fetching {year} round {round_num}. Retrying... ({retries+1}/{max_retries})")
        except requests.exceptions.RequestException as e:
            print(f"Request error fetching race data for {year} round {round_num}: {e}")
            break 

        retries += 1
        time.sleep(backoff)
        backoff *= 2

    return None



def fetch_race_results(start_year=2014, end_year=2024):
    all_results = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []

        for year in range(start_year, end_year + 1):
            season_url = f'http://ergast.com/api/f1/{year}.json'
            season_response = requests.get(season_url)
            if season_response.status_code == 200:
                races = season_response.json()['MRData']['RaceTable']['Races']

                for race in races:
                    round_num = race['round']
                    futures.append(executor.submit(fetch_single_race, year, round_num))

        for future in futures:
            try:
                result = future.result(timeout=20)
                if result:
                    all_results.extend(result)
            except TimeoutError:
                print("Timeout occurred while fetching a race result")
            except Exception as e:
                print(f"An error occurred: {e}")

    return all_results

