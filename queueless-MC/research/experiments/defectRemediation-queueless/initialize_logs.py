import pandas as pd

def initialize_logs():
    logs_df = pd.read_pickle('logs_df.pkl') # existing df
    