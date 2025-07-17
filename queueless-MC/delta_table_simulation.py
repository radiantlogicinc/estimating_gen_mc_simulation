import numpy as np
import polars as pl
import sys
from ControlTypes import *
# from DeltaSimulation import DefectType

def delta_table_simulation(control_type, log_df, deltas_df, empirical_dict, incoming_dict, outgoing_dict):
    """
    Builds delta table of time differences between state changes based on empirical log data
    
    Args
        :control_type (str):
        :log_df (Polars DataFrame):
        :deltas_df (Polars DataFrame):
        :empirical_dict (dict):
        :incoming_dict (dict):
        :outgoing_dict (dict):
    Returns
        :deltas_df (Polars DataFrame):
        :empirical_dict (dict):
        :incoming_dict (dict):
        :outgoing_dict (dict):
    """
    sub_log_df = log_df.filter(pl.col('Control_Type') == control_type)
    instance = getattr(sys.modules[__name__], control_type)(sub_log_df)
    sub_deltas_df, empirical_dict = instance.update_delta_table(empirical_dict)
    deltas_df = pl.concat([deltas_df, sub_deltas_df])
    incoming_dict, outgoing_dict, empirical_dict = instance.update_histograms(deltas_df, incoming_dict, outgoing_dict, empirical_dict)
    # questions = instance.answerQuestions(deltas_df, incoming_dict, outgoing_dict)
    return deltas_df, empirical_dict, incoming_dict, outgoing_dict
