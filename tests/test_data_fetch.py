import os

def test_file_exists():
    assert os.path.exists('./car_data.csv')
    assert os.path.exists('./driver_data.csv')
    assert os.path.exists('./race_data.csv')
