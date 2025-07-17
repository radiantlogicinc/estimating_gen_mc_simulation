import polars as pl
import pickle

def initialize_simulation(args):
    """
    Initializes delta table simulation with empirical log data
    
    Args
        :args (str):
    Returns
        :log_df (Polars DataFrame):
        :deltas_df (Polars DataFrame):
        :empirical_dict (dict):
        :incoming_dict (dict):
        :outgoing_dict (dict):
        :control_types (list):
    """
    log_df = pl.read_csv(args.path_logs)
    log_df = log_df.with_columns(pl.col('Timestamp').str.strptime(pl.Datetime, '%Y-%d-%m %H:%M:%S'))
    log_df = log_df.with_columns(pl.col("Timestamp").dt.date().alias("Date"))
    log_df = log_df.with_columns(pl.col("Timestamp").dt.hour().alias("Hour"))

    if args.path_empirical_dict is None:
        empirical_dict = {}
    else:
        with open(args.path_empirical_dict, 'rb') as f:
            empirical_dict = pickle.load(f)
    
    if args.path_incoming_dict is None:
        incoming_dict = {}
    else:
        with open(args.path_incoming_dict, 'rb') as f:
            incoming_dict = pickle.load(f)
    
    if args.path_outgoing_dict is None:
        outgoing_dict = {}
    else:
        with open(args.path_outgoing_dict, 'rb') as f:
            outgoing_dict = pickle.load(f)
    
    if args.path_deltas_df is None:
        deltas_df = pl.DataFrame(schema=[('Defect_ID',int),('Control_Type',str),('Delta_New_Assign',float),('Delta_Assign_InProgress',float),('Delta_InProgress_Closed',float), ('Delta_New_Closed',float)])
    else: ##### TO DO - update for dataframe import
        with open(args.path_deltas_df, 'rb') as f:
            deltas_df = pickle.load(f)

    control_types = log_df['Control_Type'].unique()
    for control_type in control_types:
        empirical_dict[control_type] = {'incoming_per_hour': [],
                                        'outgoing_per_hour': [],
                                        'delta_new_assign': [],
                                        'delta_assign_inprogress': [],
                                        'delta_inprogress_closed': [],
                                        'delta_new_closed': []
                                    }
        incoming_dict[control_type] = {}
        outgoing_dict[control_type] = {}

    return log_df, deltas_df, empirical_dict, incoming_dict, outgoing_dict, control_types