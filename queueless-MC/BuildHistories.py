import polars as pl
import numpy as np
import matplotlib.pyplot as plt
import datetime
from utils.VisualizationFunctions import VisualizationFunctions


class BuildHistories:
    def __init__(self, control_type, sub_log_df, sub_deltas_df, incoming_dict, outgoing_dict, timedeltas_dict, empirical_dict):
        self.control_type = control_type
        # self.sub_log_df = sub_log_df
        self.sub_log_df_inc = sub_log_df.filter(pl.col('State') == "new")
        self.sub_log_df_out = sub_log_df.filter(pl.col('State') == "closed")
        self.sub_deltas_df = sub_deltas_df
        self.incoming_dict = incoming_dict
        self.outgoing_dict = outgoing_dict
        self.timedeltas_dict = timedeltas_dict
        self.empirical_dict = empirical_dict
        self.VisualizationFunctions = VisualizationFunctions()

    def update_delta_histograms(self):
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
        plt.savefig(f'figures/{self.control_type}_delta_histograms_{datetime.date.today()}.png', bbox_inches='tight')
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
            self.incoming_dict[self.control_type].update({day.strftime('%Y-%m-%d'): incoming_defects})
            self.empirical_dict[self.control_type]['incoming_per_hour'] = self.empirical_dict[self.control_type]['incoming_per_hour'] + self.incoming_dict[self.control_type][day.strftime('%Y-%m-%d')]
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
            self.outgoing_dict[self.control_type].update({day.strftime('%Y-%m-%d'): outgoing_defects})
            self.empirical_dict[self.control_type]['outgoing_per_hour'] = self.empirical_dict[self.control_type]['outgoing_per_hour'] + self.outgoing_dict[self.control_type][day.strftime('%Y-%m-%d')]
        return self.outgoing_dict, self.empirical_dict
    
    def update_incoming_outgoing_histograms(self):
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
        plt.savefig(f'figures/{self.control_type}_histograms_{datetime.date.today()}.png', bbox_inches='tight')
        plt.close()


    def update_timedeltas(self):
        pass
        # current_date = datetime.date(2025, 7, 27) # to be replaced with .now() for real data
        # pl.date_range(self.current_date-datetime.timedelta(days=1),self.current_date,datetime.timedelta(days=1),eager=True)
        # for day in self.sub_log_df["Date"].unique():
        #     pass


    def update_timeline_figures(self):
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
        plt.savefig(f'figures/{self.control_type}_fig_{datetime.date.today()}.png', bbox_inches='tight')
        plt.close()