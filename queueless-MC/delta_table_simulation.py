import numpy as np
import polars as pl
from ControlTypes import *
# from DeltaSimulation import DefectType

def delta_table_simulation(control_type, log_df, deltas_df, empirical_dict, incoming_dict, outgoing_dict):
    sub_log_df = log_df.filter(pl.col('Control_Type') == control_type)
    exec(f"instance = {control_type}(sub_log_df)")
    sub_deltas_df, empirical_dict = instance.update_delta_table(empirical_dict)
    deltas_df = pl.concat([deltas_df, sub_deltas_df])
    incoming_dict, outgoing_dict, empirical_dict = instance.update_histograms(deltas_df, incoming_dict, outgoing_dict, empirical_dict)
    # questions = instance.answerQuestions(deltas_df, incoming_dict, outgoing_dict)
    return deltas_df
