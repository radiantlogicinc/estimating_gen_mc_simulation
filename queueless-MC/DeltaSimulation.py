import polars as pl
import numpy as np
import queue
import contextlib
import matplotlib.pyplot as plt
from datetime import date
from abc import ABC
from ..utils import VisualizationFunctions


class LogEntry:
    def __init__(self, row_id, sub_log_df, sub_deltas_dict):
        self.defect_id = sub_log_df.filter(pl.col('ID')==row_id)['Defect_ID'][0]
        self.state = sub_log_df.filter(pl.col('ID')==row_id)['State'][0]
        self.control_type = sub_log_df.filter(pl.col('ID')==row_id)['Control_Type'][0]
        self.sub_log_df = sub_log_df
        self.sub_deltas_dict = sub_deltas_dict

    def check_state(self, empirical_dict):
        """
        For each row of log data, identifies state change and computes time delta between state changes if applicable
        
        Args
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type)
        Returns
            :sub_deltas_dict (dict): intermediary containing single row corresponding to one defect, to be appended to deltas_df
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type) (adjusted)
        """
        if self.state == 'new':
            ## append to delta table with defect_ID and control_type
            self.sub_deltas_dict[self.defect_id] = {'Defect_ID': self.defect_id,
                                                    'Control_Type': self.control_type,
                                                    'Delta_New_Assign': float('nan'),
                                                    'Delta_Assign_InProgress': float('nan'),
                                                    'Delta_InProgress_Closed': float('nan'),
                                                    'Delta_New_Closed': float('nan'),
                                                   }

        elif self.state =='assign':
            delta_new_assign, empirical_dict = self.state_assign(empirical_dict)
            self.sub_deltas_dict[self.defect_id].update({'Delta_New_Assign': delta_new_assign})
    
        elif self.state == 'in-progress':
            delta_assign_inprogress, empirical_dict = self.state_inprogress(empirical_dict)
            self.sub_deltas_dict[self.defect_id].update({'Delta_Assign_InProgress': delta_assign_inprogress})
        
        elif self.state == 'closed':
            delta_inprogress_closed, delta_new_closed, empirical_dict = self.state_closed(empirical_dict)
            self.sub_deltas_dict[self.defect_id].update({'Delta_InProgress_Closed': delta_inprogress_closed, 'Delta_New_Closed': delta_new_closed})  

        return self.sub_deltas_dict, empirical_dict


    def state_assign(self, empirical_dict):
        """
        Computes time difference between 'new' and 'assign' states in log data
        
        Args
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type)
        Returns
            :delta_new_assign (Polars datetime): time difference between 'new' and 'assign' states in log data
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type) (adjusted)
        """
        timestamp_new = self.sub_log_df.filter(pl.col('Defect_ID')==self.defect_id, pl.col('State')=='new')['Timestamp'][0]
        timestamp_assign = self.sub_log_df.filter(pl.col('Defect_ID')==self.defect_id, pl.col('State')==self.state)['Timestamp'][0]
        
        ## time between assign and new (hrs)
        delta_new_assign = self.compute_delta(timestamp_new, timestamp_assign)
        
        ## append value to empirical dict
        empirical_dict[self.control_type]['delta_new_assign'].append(delta_new_assign)
        return delta_new_assign, empirical_dict
    
    def state_inprogress(self, empirical_dict):
        """
        Computes time difference between 'assign' and 'in-progress' states in log data
        
        Args
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type)
        Returns
            :delta_assign_inprogress (Polars datetime): time difference between 'assign' and 'in-progress' states in log data
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type) (adjusted)
        """
        timestamp_assign = self.sub_log_df.filter(pl.col('Defect_ID')==self.defect_id, pl.col('State')=='assign')['Timestamp'][0]
        timestamp_inprogress = self.sub_log_df.filter(pl.col('Defect_ID')==self.defect_id, pl.col('State')==self.state)['Timestamp'][0]
        
        ## time between in-progress and assign (hrs)
        delta_assign_inprogress = self.compute_delta(timestamp_assign, timestamp_inprogress)
        
        ## append value to empirical dict
        empirical_dict[self.control_type]['delta_assign_inprogress'].append(delta_assign_inprogress)
        return delta_assign_inprogress, empirical_dict
    
    def state_closed(self, empirical_dict):
        """
        Computes time difference between 'in-progress' and 'closed', and 'new' and 'closed' states in log data
        
        Args
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type)
        Returns
            :delta_inprogress_closed (Polars datetime): time difference between 'in-progress' and 'closed' states in log data
            :delta_new_closed (Polars datetime): time difference between 'new' and 'closed' states in log data
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type) (adjusted)
        """
        timestamp_new = self.sub_log_df.filter(pl.col('Defect_ID')==self.defect_id, pl.col('State')=='new')['Timestamp'][0]
        timestamp_inprogress = self.sub_log_df.filter(pl.col('Defect_ID')==self.defect_id, pl.col('State')=='in-progress')['Timestamp'][0]
        timestamp_closed = self.sub_log_df.filter(pl.col('Defect_ID')==self.defect_id, pl.col('State')==self.state)['Timestamp'][0]
        
        ## time between closed and in-progress, and closed and new (hrs)
        delta_inprogress_closed = self.compute_delta(timestamp_inprogress, timestamp_closed)
        delta_new_closed = self.compute_delta(timestamp_new, timestamp_closed)
        
        ## append value to empirical dict
        empirical_dict[self.control_type]['delta_inprogress_closed'].append(delta_inprogress_closed)
        empirical_dict[self.control_type]['delta_new_closed'].append(delta_new_closed)
        return delta_inprogress_closed, delta_new_closed, empirical_dict
    
    def compute_delta(self, timestamp_older, timestamp_newer):
        """
        Computes time difference between two timestamps
        
        Args
            :timestamp_older (Polars datetime): older of two timestamps for computing time difference
            :timestamp_newer (Polars datetime): more recent of two timestamps for computing time difference
        Returns
            :delta_timestamps (Polars datetime): time difference between two timestamps 
        """
        ## Computes the time delta (in hrs) as timestamp_newer - timestamp_older
        delta_timestamps = timestamp_newer - timestamp_older
        delta_timestamps = round(delta_timestamps.total_seconds()/3600, 3)
        return delta_timestamps
    
class BuildHistories:
    def __init__(self, control_type, sub_log_df, sub_deltas_df, incoming_dict, outgoing_dict, empirical_dict):
        self.control_type = control_type
        self.sub_log_df_inc = sub_log_df.filter(pl.col('State') == "new")
        self.sub_log_df_out = sub_log_df.filter(pl.col('State') == "closed")
        self.sub_deltas_df = sub_deltas_df
        self.incoming_dict = incoming_dict
        self.outgoing_dict = outgoing_dict
        self.empirical_dict = empirical_dict
        self.VisualizationFunctions = VisualizationFunctions()

    def update_figures(self):
        """
        Updates timeline figures tracking evolution of time deltas (per defect type)
        """
        fig, axs = plt.subplots(1, 4, figsize=(16, 4))
        deltas = ['Delta_New_Assign', 'Delta_Assign_InProgress', 'Delta_InProgress_Closed', 'Delta_New_Closed']
        ylabels = ['time (hrs)', None, None, None]
        for index in range(4):
            xdata = range(1,len(self.sub_deltas_df.select(deltas[index])) + 1)
            ydata = self.sub_deltas_df.select(deltas[index])
            fig, axs = self.VisualizationFunctions.visualize_timeline(fig,
                                                                      axs,
                                                                      index,
                                                                      xdata,
                                                                      ydata,
                                                                      ylabel=ylabels[index],
                                                                      title=deltas[index])
    
        fig.text(0.5, 0.95, f'{self.control_type}', ha='center', fontsize=12)
        plt.savefig(f'figures/{self.control_type}_fig_{date.today()}.png', bbox_inches='tight')
        plt.close()

    def update_incoming(self):
        """
        Measures and stores incoming defects per hour (per defect type) 
        
        Returns
            :incoming_dict (dict): tracks incoming defects per hour (per defect type)
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type)
        """
        for day in self.sub_log_df_inc["Date"].unique():
            sub_log_df_inc_date = self.sub_log_df_inc.filter(pl.col('Date') == day)['Hour'].value_counts()
            incoming_defects = [sub_log_df_inc_date.filter(pl.col('Hour') == i)['count'][0] if i in list(sub_log_df_inc_date.select('Hour'))[0] else 0 for i in range(24)]
            self.incoming_dict[self.control_type].update({day: incoming_defects})
            self.empirical_dict[self.control_type]['incoming_per_hour'] = self.empirical_dict[self.control_type]['incoming_per_hour'] + self.incoming_dict[self.control_type][day]
        return self.incoming_dict, self.empirical_dict
    
    def update_outgoing(self):
        """
        Measures and stores outgoing defects per hour (per defect type)
        
        Returns
            :outgoing_dict (dict): tracks outgoing defects per hour (per defect type)
            :empirical_dict (dict): tracks incoming/outgoing and delta histograms from empirical data (per defect type)
        """
        for day in self.sub_log_df_out["Date"].unique(): # will never be empty 
            sub_log_df_out_date = self.sub_log_df_out.filter(pl.col('Date') == day)['Hour'].value_counts()
            outgoing_defects = [sub_log_df_out_date.filter(pl.col('Hour') == i)['count'][0] if i in list(sub_log_df_out_date.select('Hour'))[0] else 0 for i in range(24)]
            self.outgoing_dict[self.control_type].update({day: outgoing_defects})
            self.empirical_dict[self.control_type]['outgoing_per_hour'] = self.empirical_dict[self.control_type]['outgoing_per_hour'] + self.outgoing_dict[self.control_type][day]
        return self.outgoing_dict, self.empirical_dict

    def update_delta_distributions(self):
        """
        Updates empirical log data delta histograms (per defect type)
        """
        fig, axs = plt.subplots (1, 4, figsize=(16,4))
        deltas = ['Delta_New_Assign', 'Delta_Assign_InProgress', 'Delta_InProgress_Closed', 'Delta_New_Closed']
        for index in range(4):
            data = self.empirical_dict[self.control_type][deltas[index].lower()]
            fig, axs = self.VisualizationFunctions.visualize_histograms_theory(fig,
                                                                               axs,
                                                                               index,
                                                                               data,
                                                                               label=None,
                                                                               title=deltas[index],
                                                                               color='#C21445')
        plt.savefig(f'figures/{self.control_type}_delta_histograms_{date.today()}.png', bbox_inches='tight')
        plt.close()
    
    def update_incoming_outgoing_distributions(self):
        """
        Updates empirical log data incoming + outgoing histograms (per defect type)
        """
        fig, axs = plt.subplots (1, 2, figsize=(16,4))
        # incoming_per_hour histogram
        data_incoming = np.array(self.empirical_dict[self.control_type]['incoming_per_hour'])
        fig, axs = self.VisualizationFunctions.visualize_histograms_theory(fig,
                                                                           axs,
                                                                           0,
                                                                           data_incoming,
                                                                           label='incoming',
                                                                           title=f'{self.control_type} incoming [/hr]',
                                                                           color='#C21445')
        # outgoing_per_hour histogram
        data_outgoing = np.array(self.empirical_dict[self.control_type]['outgoing_per_hour'])
        fig, axs = self.VisualizationFunctions.visualize_histograms_theory(fig,
                                                                           axs,
                                                                           1,
                                                                           data_outgoing,
                                                                           label='outgoing',
                                                                           title=f'{self.control_type} outgoing [/hr]',
                                                                           color='#C21445')
        plt.savefig(f'figures/{self.control_type}_histograms_{date.today()}.png', bbox_inches='tight')
        plt.close()


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
        self.sub_deltas_df = pl.DataFrame(schema=[('Defect_ID',int),('Control_Type',str),('Delta_New_Assign',float),('Delta_Assign_InProgress',float),('Delta_InProgress_Closed',float), ('Delta_New_Closed',float)])
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

    def update_histograms(self, deltas_df, incoming_dict, outgoing_dict, empirical_dict):
        """
        Triggers empirical log data histograms update (per defect type)
        
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
        sub_deltas_df = deltas_df.filter(pl.col('Control_Type') == self.control_type).drop_nans()
        histories = BuildHistories(self.control_type, self.sub_log_df, sub_deltas_df, incoming_dict, outgoing_dict, empirical_dict)
        if sub_deltas_df.is_empty() == 0:
            histories.update_figures() ## update the delta figures for this control type
            incoming_dict, empirical_dict = histories.update_incoming() ## update incoming defect distributions for this control_type
            outgoing_dict, empirical_dict = histories.update_outgoing() ## update outgoing defect distributions for this control_type
            histories.update_distributions()
        return incoming_dict, outgoing_dict, empirical_dict

    # def answer_questions(self, deltas_df, incoming_dict, outgoing_dict):
    #     sub_deltas_df = deltas_df.filter(pl.col('Control_Type') == self.control_type).drop_nans()
    #     questions = answeringQuestions(self.control_type, self.sub_log_df, sub_deltas_df, incoming_dict, outgoing_dict)
    #     return questions