import pandas as pd
import requests
from data_fetch.compile_race_data import fetch_race_results
from data_fetch.compile_driver_data import fetch_driver_performance
from data_fetch.compile_car_data import fetch_car_performance

def main():
    race_data = fetch_race_results(start_year=2014, end_year=2024)
    race_df = pd.DataFrame(race_data)
    race_df.to_csv('race_data.csv', index=False)
    print("Race data saved to race_data.csv")

    driver_data = fetch_driver_performance(start_year=2014, end_year=2024)
    driver_df = pd.DataFrame(driver_data)
    driver_df.to_csv('driver_data.csv', index=False)
    print("Driver data saved to driver_data.csv")

    car_data = fetch_car_performance(start_year=2018, end_year=2018)
    car_df = pd.DataFrame(car_data)
    car_df.to_csv('car_data.csv', index=False)
    print("Car data saved to car_data.csv")

if __name__ == "__main__":
    main()
