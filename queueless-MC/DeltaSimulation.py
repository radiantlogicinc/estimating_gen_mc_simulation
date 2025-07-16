import polars as pl
import numpy as np
import queue
import contextlib
import matplotlib.pyplot as plt
from datetime import date
from abc import ABC


class LogEntry:
    def __init__(self, row_id, sub_log_df, sub_deltas_dict):
        self.defect_id = sub_log_df.filter(pl.col('ID')==row_id)['Defect_ID'][0]
        self.state = sub_log_df.filter(pl.col('ID')==row_id)['State'][0]
        self.control_type = sub_log_df.filter(pl.col('ID')==row_id)['Control_Type'][0]
        self.sub_log_df = sub_log_df
        self.sub_deltas_dict = sub_deltas_dict

    def check_state(self, empirical_dict):
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
        timestamp_new = self.sub_log_df.filter(pl.col('Defect_ID')==self.defect_id, pl.col('State')=='new')['Timestamp'][0]
        timestamp_assign = self.sub_log_df.filter(pl.col('Defect_ID')==self.defect_id, pl.col('State')==self.state)['Timestamp'][0]
        
        ## time between assign and new (hrs)
        delta_new_assign = self.compute_delta(timestamp_new, timestamp_assign)
        
        ## append value to empirical dict
        empirical_dict[self.control_type]['delta_new_assign'].append(delta_new_assign)
        return delta_new_assign, empirical_dict
    
    def state_inprogress(self, empirical_dict):
        timestamp_assign = self.sub_log_df.filter(pl.col('Defect_ID')==self.defect_id, pl.col('State')=='assign')['Timestamp'][0]
        timestamp_inprogress = self.sub_log_df.filter(pl.col('Defect_ID')==self.defect_id, pl.col('State')==self.state)['Timestamp'][0]
        
        ## time between in-progress and assign (hrs)
        delta_assign_inprogress = self.compute_delta(timestamp_assign, timestamp_inprogress)
        
        ## append value to empirical dict
        empirical_dict[self.control_type]['delta_assign_inprogress'].append(delta_assign_inprogress)
        return delta_assign_inprogress, empirical_dict
    
    def state_closed(self, empirical_dict):
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

    def update_figures(self):
        fig, axs = plt.subplots(1, 4, figsize=(16, 4))
        axs[0].plot(range(1,len(self.sub_deltas_df.select('Delta_New_Assign'))+1), self.sub_deltas_df.select('Delta_New_Assign'), 'k-o', markersize=6)
        axs[0].set_ylabel('time (hrs)')
        axs[0].set_title('Delta_New_Assign', loc='left', fontsize=10)

        axs[1].plot(range(1,len(self.sub_deltas_df.select('Delta_Assign_InProgress'))+1), self.sub_deltas_df.select('Delta_Assign_InProgress'), 'k-o', markersize=6)
        axs[1].set_title('Delta_Assign_InProgress', loc='left', fontsize=10)

        axs[2].plot(range(1,len(self.sub_deltas_df.select('Delta_InProgress_Closed'))+1), self.sub_deltas_df.select('Delta_InProgress_Closed'), 'k-o', markersize=6)
        axs[2].set_title('Delta_InProgress_Closed', loc='left', fontsize=10)

        axs[3].plot(range(1,len(self.sub_deltas_df.select('Delta_New_Closed'))+1), self.sub_deltas_df.select('Delta_New_Closed'), 'k-o', markersize=6)
        axs[3].set_title('Delta_New_Closed', loc='left', fontsize=10)
    
        fig.text(0.5, 0.95, f'{self.control_type}', ha='center', fontsize=12)
        plt.savefig(f'figures/{self.control_type}_fig_{date.today()}.png', bbox_inches='tight')
        plt.close()

    def update_incoming(self):
        for day in self.sub_log_df_inc["Date"].unique(): # TO DO - how to track date outside of logs (i.e. if no state changes/defects generated, dates do not appear in logs 
            sub_log_df_inc_date = self.sub_log_df_inc.filter(pl.col('Date') == day)['Hour'].value_counts()
            incoming_defects = [sub_log_df_inc_date.filter(pl.col('Hour') == i)['count'][0] if i in list(sub_log_df_inc_date.select('Hour'))[0] else 0 for i in range(24)]
            self.incoming_dict[self.control_type].update({day: incoming_defects})
            self.empirical_dict[self.control_type]['incoming_per_hour'] = self.empirical_dict[self.control_type]['incoming_per_hour'] + self.incoming_dict[self.control_type][day]
        return self.incoming_dict, self.empirical_dict
    
    def update_outgoing(self):
        for day in self.sub_log_df_out["Date"].unique(): # will never be empty 
            sub_log_df_out_date = self.sub_log_df_out.filter(pl.col('Date') == day)['Hour'].value_counts()
            outgoing_defects = [sub_log_df_out_date.filter(pl.col('Hour') == i)['count'][0] if i in list(sub_log_df_out_date.select('Hour'))[0] else 0 for i in range(24)]
            self.outgoing_dict[self.control_type].update({day: outgoing_defects})
            self.empirical_dict[self.control_type]['outgoing_per_hour'] = self.empirical_dict[self.control_type]['outgoing_per_hour'] + self.outgoing_dict[self.control_type][day]
        return self.outgoing_dict, self.empirical_dict

    def update_distributions(self):
        csfont = {'fontname':'Arial'}
        fig, axs = plt.subplots (1, 5, figsize=(16,4))
        # incoming_per_hour histogram
        data_incoming = np.array(self.empirical_dict[self.control_type]['incoming_per_hour'])
        unique_values = np.unique(data_incoming)
        d = 1 if np.all(unique_values) == 0 else np.diff(unique_values).min()
        left_of_first_bin = data_incoming.min() - float(d)/2
        right_of_last_bin = data_incoming.max() + float(d)/2
        axs[0].hist(data_incoming, np.arange(left_of_first_bin, right_of_last_bin + d, d), label='incoming', color='maroon', alpha=0.2, edgecolor='black', linewidth=1.5, density=True)
        axs[0].set_title(f'{self.control_type}', loc='left', fontsize=14, **csfont)
    
        # outgoing_per_hour histogram 
        data_outgoing = np.array(self.empirical_dict[self.control_type]['outgoing_per_hour'])
        unique_values = np.unique(data_outgoing)
        d = 1 if np.all(unique_values) == 0 else np.diff(unique_values).min()
        left_of_first_bin = data_outgoing.min() - float(d)/2
        right_of_last_bin = data_outgoing.max() + float(d)/2
        axs[0].hist(data_outgoing, np.arange(left_of_first_bin, right_of_last_bin + d, d), label='outgoing', histtype='step', linewidth=1.5, density=True)
        # axs[index][0].set_title(f'{value}', loc='left', fontsize=14, **csfont)
        axs[0].legend(fontsize='small')
        
        # delta histograms
        axs[1].hist(self.empirical_dict[self.control_type]['delta_new_assign'], bins=3, edgecolor='black', linewidth=1.5, density=True)
        axs[2].hist(self.empirical_dict[self.control_type]['delta_assign_inprogress'], bins=3, edgecolor='black', linewidth=1.5, density=True)
        axs[3].hist(self.empirical_dict[self.control_type]['delta_inprogress_closed'], bins=3, edgecolor='black', linewidth=1.5, density=True)
        axs[4].hist(self.empirical_dict[self.control_type]['delta_new_closed'], bins=3, edgecolor='black', linewidth=1.5, density=True)
    
        axs[1].set_title('Delta_New_Assign', loc='left', fontsize=12, **csfont)
        axs[2].set_title('Delta_Assign_InProgress', loc='left', fontsize=12, **csfont)
        axs[3].set_title('Delta_InProgress_Closed', loc='left', fontsize=12, **csfont)
        axs[4].set_title('Delta_New_Closed', loc='left', fontsize=12, **csfont)
        
        # plt.savefig(f'figures/{self.control_type}_histograms_{date.today()}.png', bbox_inches='tight')
        plt.close()


class DefectType(ABC):
    def __init__(self, control_type, sub_log_df):
        self.control_type = control_type
        self.sub_log_df = sub_log_df

    def update_delta_table(self, empirical_dict):
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