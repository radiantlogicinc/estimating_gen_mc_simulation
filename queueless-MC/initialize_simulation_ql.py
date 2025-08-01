import polars as pl
import pickle
import json
import datetime

def initialize_simulation(args):
    """
    Initializes delta table simulation with empirical log data and initializes empirical_dict, incoming_dict,
    outgoing_dict, deltas_df with existing values if specified
    
    Args
        :args (argparse parser.parse_args()): parsed input arguments
    Returns
        :log_df (Polars DataFrame): log data tracking state changes during a defect's lifetime
        :deltas_df (Polars DataFrame): delta table tracking time deltas between state changes (per defect)
        :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type)
        :incoming_dict (dict): tracks incoming defects per hour (per defect type)
        :outgoing_dict (dict): tracks outgoing defects per hour (per defect type)
        :control_types (list): list of control types in log data
    """
    log_df = pl.read_csv(args.path_logs)
    log_df = log_df.with_columns(pl.col('Timestamp').str.strptime(pl.Datetime, '%Y-%d-%m %H:%M:%S'))
    log_df = log_df.with_columns(pl.col("Timestamp").dt.date().alias("Date"))
    log_df = log_df.with_columns(pl.col("Timestamp").dt.hour().alias("Hour"))

    # if args.path_empirical_dict is None:
    #     empirical_dict = {}
    # else:
    with open('simulations/dicts/empirical_dict.json', 'r') as f:
        empirical_dict = json.load(f)
    # with open('simulations/dicts/empirical_dict.pkl', 'r') as f:
    #     empirical_dict = pickle.load(f)
    
    # if args.path_incoming_dict is None:
    #     incoming_dict = {}
    # else:
    with open('simulations/dicts/incoming_dict.json', 'r') as f:
        incoming_dict = json.load(f)
    # incoming_dict = {datetime.datetime.strptime(key, "%Y-%m-%d").date(): value for key, value in }
    
    # if args.path_outgoing_dict is None:
    #     outgoing_dict = {}
    # else:
    with open('simulations/dicts/outgoing_dict.json', 'r') as f:
        outgoing_dict = json.load(f)

    with open('simulations/dicts/timedeltas_dict.json', 'r') as f:
        timedeltas_dict = json.load(f)
    
    # if args.path_deltas_df is None:
    #     deltas_df = pl.DataFrame(schema=[('Defect_ID',int),('Control_Type',str),('Delta_New_Assign',float),('Delta_Assign_InProgress',float),('Delta_InProgress_Closed',float), ('Delta_New_Closed',float)])
    # else:
    # deltas_df = pl.read_csv('deltas_df.csv') # to be moved to json
    with open('simulations/deltas_df.json', 'rb') as file:
        deltas_df = pl.read_json(file)
    deltas_df = deltas_df.with_columns(pl.col('Date_Assign').str.strptime(pl.Date, '%Y-%m-%d'),
                                       pl.col('Date_InProgress').str.strptime(pl.Date, '%Y-%m-%d'),
                                       pl.col('Date_Closed').str.strptime(pl.Date, '%Y-%m-%d'))


    control_types = log_df['Control_Type'].unique()
    for control_type in control_types:
        if (not empirical_dict) or (control_type not in empirical_dict.keys()):
            empirical_dict[control_type] = {'incoming_per_hour': [],
                                            'outgoing_per_hour': [],
                                            'delta_new_assign': [],
                                            'delta_assign_inprogress': [],
                                            'delta_inprogress_closed': [],
                                            'delta_new_closed': []
                                    }
        if (not incoming_dict) or (control_type not in incoming_dict.keys()):
            incoming_dict[control_type] = {}
        if (not outgoing_dict) or (control_type not in outgoing_dict.keys()):
            outgoing_dict[control_type] = {}
        if (not timedeltas_dict) or (control_type not in timedeltas_dict.keys()):
            timedeltas_dict[control_type] = {'delta_new_assign': {},
                                            'delta_assign_inprogress': {},
                                            'delta_inprogress_closed': {},
                                            'delta_new_closed': {}
                                            }

    return log_df, deltas_df, empirical_dict, incoming_dict, outgoing_dict, timedeltas_dict, control_types