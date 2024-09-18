import requests
import time
from concurrent.futures import ThreadPoolExecutor

def fetch_single_driver_data(year, round_num, max_retries=3):
    url = f'http://ergast.com/api/f1/{year}/{round_num}/drivers.json'
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                try:
                    data = response.json()['MRData']['DriverTable']['Drivers']
                    return data
                except KeyError:
                    print(f"Unexpected response structure for {year} round {round_num}")
                    return None
        except requests.exceptions.Timeout:
            print(f"Timeout for {year} round {round_num}. Retrying...")
            retries += 1
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching driver data for {year} round {round_num}: {e}")
            break
    return None

def fetch_driver_performance(start_year=2014, end_year=2024):
    all_drivers = {}

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []

        for year in range(start_year, end_year + 1):
            season_url = f'http://ergast.com/api/f1/{year}.json'
            season_response = requests.get(season_url)
            if season_response.status_code == 200:
                races = season_response.json()['MRData']['RaceTable']['Races']

                for race in races:
                    round_num = race['round']
                    futures.append(executor.submit(fetch_single_driver_data, year, round_num))

        for future in futures:
            result = future.result()
            if result:
                for driver in result:
                    driver_id = driver['driverId']
                    if driver_id not in all_drivers:
                        all_drivers[driver_id] = {
                            'driverId': driver['driverId'],
                            'givenName': driver['givenName'],
                            'familyName': driver['familyName'],
                            'dateOfBirth': driver['dateOfBirth'],
                            'nationality': driver['nationality']
                        }

    return all_drivers.values()
