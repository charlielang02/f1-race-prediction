import fastf1 as ff1
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

ff1.Cache.enable_cache('cache')

import fastf1 as ff1
import pandas as pd

ff1.Cache.enable_cache('cache')

def fetch_car_performance_session(year, race_name):
    try:
        session = ff1.get_session(year, race_name, 'R')
        session.load()
        
        car_performance_data = []
        
        for driver in session.drivers:
            driver_laps = session.laps.pick_driver(driver)
            
            top_speed = driver_laps['SpeedST'].max()
            
            avg_top_speed = driver_laps['SpeedST'].mean()
            
            fastest_lap_time = driver_laps['LapTime'].min().total_seconds()
            
            avg_lap_time = driver_laps['LapTime'].mean().total_seconds()
            
            car_performance_data.append({
                'year': year,
                'race': race_name,
                'driver': driver,
                'top_speed': top_speed,
                'avg_top_speed': avg_top_speed,
                'fastest_lap_time': fastest_lap_time,
                'avg_lap_time': avg_lap_time
            })
        
        return car_performance_data
    
    except Exception as e:
        print(f"Failed to fetch car performance data for {race_name} ({year}): {str(e)}")
        return []


def fetch_car_performance(start_year=2013, end_year=2023):
    car_performance_data = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        
        for year in range(start_year, end_year + 1):
            season = ff1.get_event_schedule(year)
            if isinstance(season, pd.DataFrame):
                print(season.head())
                for index, event in season.iterrows():
                    race_name = event['OfficialEventName']
                    futures.append(executor.submit(fetch_car_performance_session, year, race_name))
            else:
                print(f"Unexpected format for season data: {type(season)}")

        for future in futures:
            result = future.result()
            if result:
                car_performance_data.extend(result)

    return car_performance_data