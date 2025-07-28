import polars as pl
import sys
from ControlTypes import *
# from DeltaSimulation import DefectType

def delta_table_simulation(control_type, log_df, deltas_df, empirical_dict, incoming_dict, outgoing_dict):
    """
    Builds delta table of time differences between state changes based on empirical log data
    
    Args
        :control_type (str): defect's control type
        :log_df (Polars DataFrame): log data tracking state changes during a defect's lifetime
        :deltas_df (Polars DataFrame): delta table tracking time deltas between state changes (per defect)
        :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type)
        :incoming_dict (dict): tracks incoming defects per hour (per defect type)
        :outgoing_dict (dict): tracks outgoing defects per hour (per defect type)
    Returns
        :deltas_df (Polars DataFrame): delta table tracking time deltas between state changes (per defect) (adjusted)
        :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type) (adjusted)
        :incoming_dict (dict): tracks incoming defects per hour (per defect type) (adjusted)
        :outgoing_dict (dict): tracks outgoing defects per hour (per defect type) (adjusted)
    """
    sub_log_df = log_df.filter(pl.col('Control_Type') == control_type)
    instance = getattr(sys.modules[__name__], control_type)(sub_log_df)
    sub_deltas_df, empirical_dict = instance.update_delta_table(empirical_dict)
    deltas_df = pl.concat([deltas_df, sub_deltas_df])
    incoming_dict, outgoing_dict, empirical_dict = instance.update_histograms(deltas_df, incoming_dict, outgoing_dict, empirical_dict)
    return deltas_df, empirical_dict, incoming_dict, outgoing_dict
