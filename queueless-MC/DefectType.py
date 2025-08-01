import polars as pl
import queue
import contextlib
import datetime
from LogEntry import LogEntry
from BuildHistories import BuildHistories
from abc import ABC

class DefectType(ABC):
    def __init__(self, control_type, sub_log_df):
        self.control_type = control_type
        self.sub_log_df = sub_log_df

    def update_delta_table(self, empirical_dict):
        """
        Triggers delta table update tracking time differences between state changes (per defect)
        
        Args
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type)
        Returns
            :sub_deltas_df (Polars DataFrame): subset of deltas_df (e.g. single control type)
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type) (adjusted)
        """
        processing_queue = queue.Queue()
        self.sub_deltas_df = pl.DataFrame(schema=[('Defect_ID',int),
                                                  ('Control_Type',str),
                                                  ('Delta_New_Assign',float),
                                                  ('Date_Assign', datetime.date),
                                                  ('Delta_Assign_InProgress',float),
                                                  ('Date_InProgress', datetime.date),
                                                  ('Delta_InProgress_Closed',float),
                                                  ('Delta_New_Closed',float),
                                                  ('Date_Closed', datetime.date)])
        sub_deltas_dict = {}
        for row in self.sub_log_df.iter_rows():
            with contextlib.suppress(IndexError):
                processing_queue.put(row[0], block=False)

        while processing_queue.qsize() > 0: # takes previous 24h of data
            with contextlib.suppress(queue.Empty):
                row_id = processing_queue.get(block=False)
            log_entry = LogEntry(row_id, self.sub_log_df, sub_deltas_dict)
            sub_deltas_dict, empirical_dict = log_entry.check_state(empirical_dict)
        for key in sub_deltas_dict.keys():
            self.sub_deltas_df.extend(pl.DataFrame(sub_deltas_dict[key]))
        return self.sub_deltas_df, empirical_dict

    def update_histograms(self, log_df, deltas_df, incoming_dict, outgoing_dict, timedeltas_dict, empirical_dict):
        """
        Triggers empirical log data histograms and corresponding figures update (per defect type)
        
        Args
            :deltas_df (Polars DataFrame): delta table tracking time deltas between state changes (per defect)
            :incoming_dict (dict): tracks incoming defects per hour (per defect type)
            :outgoing_dict (dict): tracks outgoing defects per hour (per defect type)
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type)
        Returns
            :incoming_dict (dict): tracks incoming defects per hour (per defect type) (adjusted)
            :outgoing_dict (dict): tracks outgoing defects per hour (per defect type) (adjusted)
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type) (adjusted)
        """
        # sub_deltas_df = deltas_df.filter(pl.col('Control_Type') == self.control_type).drop_nans()
        sub_deltas_df = deltas_df.filter(pl.col('Control_Type') == self.control_type)
        histories = BuildHistories(self.control_type, log_df, sub_deltas_df, incoming_dict, outgoing_dict, timedeltas_dict, empirical_dict)
        if sub_deltas_df.is_empty() == 0:
            histories.update_empirical()
            histories.update_delta_histograms()
            incoming_dict, empirical_dict = histories.update_incoming()
            outgoing_dict, empirical_dict = histories.update_outgoing()
            histories.update_incoming_outgoing_histograms()
            histories.update_timedeltas()
            histories.update_timeline_figures()
        return incoming_dict, outgoing_dict, timedeltas_dict, empirical_dict