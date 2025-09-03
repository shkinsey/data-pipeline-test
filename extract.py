import pandas as pd

def extract(csv_path):
    
#TODO: add error handling 

    return pd.read_csv(csv_path, parse_dates=['timestamp'])
    

