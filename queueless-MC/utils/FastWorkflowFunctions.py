import polars as pl
import numpy as np
import datetime

class FastWorkflowFunctions:
    def __init__(self, sub_dict, control_type, sub_deltas_df, incoming_dict, outgoing_dict):
        self.sub_dict = sub_dict
        self.control_type = control_type
        self.sub_deltas_df = sub_deltas_df
        self.incoming_dict = incoming_dict
        self.outgoing_dict = outgoing_dict
        self.current_date = datetime.date(2025, 7, 27) # to be replaced with .now() for real data
    
    def update_sub_dict(self, item, time_span, mean):
        self.sub_dict[f'{item}_{self.control_type}_{time_span}'] = {'item': f'{item}_{self.control_type}_{time_span}',
                                                                    'control_type': self.control_type,
                                                                    'time_span': time_span,
                                                                    'mean': mean}

    def run_fastworkflow_functions(self):
        self.generated_per_day()
        self.remediated_per_day()
        self.delta_new_assign()
        self.delta_assign_inprogress()
        # self.delta_inprogress_closed()
        # self.delta_new_closed()
        return self.sub_dict

    def generated_per_day(self):
        item = 'generated_per_day'
        self.update_sub_dict(item, '1', str(round(np.mean([np.mean(self.incoming_dict[self.control_type][key])
                                                           for key in pl.date_range(self.current_date-datetime.timedelta(days=1),self.current_date,datetime.timedelta(days=1),eager=True)
                                                           if key in self.incoming_dict[self.control_type]]), 3)))
        self.update_sub_dict(item, '2', str(round(np.mean([np.mean(self.incoming_dict[self.control_type][key])
                                                           for key in pl.date_range(self.current_date-datetime.timedelta(days=90),self.current_date,datetime.timedelta(days=1),eager=True)
                                                           if key in self.incoming_dict[self.control_type]]), 3)))
        self.update_sub_dict(item, '3', str(round(np.mean([np.mean(self.incoming_dict[self.control_type][key])
                                                           for key in self.incoming_dict[self.control_type].keys()]), 3)))

    def remediated_per_day(self):
        item = 'remediated_per_day'
        self.update_sub_dict(item, '1', str(round(np.mean([np.mean(self.outgoing_dict[self.control_type][key])
                                                           for key in pl.date_range(self.current_date-datetime.timedelta(days=1),self.current_date,datetime.timedelta(days=1),eager=True)
                                                           if key in self.outgoing_dict[self.control_type]]), 3)))
        self.update_sub_dict(item, '2', str(round(np.mean([np.mean(self.outgoing_dict[self.control_type][key])
                                                           for key in pl.date_range(self.current_date-datetime.timedelta(days=90),self.current_date,datetime.timedelta(days=1),eager=True)
                                                           if key in self.outgoing_dict[self.control_type]]), 3)))
        self.update_sub_dict(item, '3', str(round(np.mean([np.mean(self.outgoing_dict[self.control_type][key])
                                                           for key in self.outgoing_dict[self.control_type].keys()]), 3)))

    def delta_new_assign(self):
        item = 'delta_new_assign'
        self.update_sub_dict(item, '1', str(self.sub_deltas_df.filter(pl.col('Date_Assign').is_between(self.current_date-datetime.timedelta(days=1),
                                                                   self.current_date,
                                                                   closed = 'both')).mean()['Delta_New_Assign'][0]))
        self.update_sub_dict(item, '2', str(self.sub_deltas_df.filter(pl.col('Date_Assign').is_between(self.current_date-datetime.timedelta(days=90),
                                                                   self.current_date,
                                                                   closed = 'both')).mean()['Delta_New_Assign'][0]))
        self.update_sub_dict(item, '3', str(self.sub_deltas_df.mean()['Delta_New_Assign'][0]))
        

    def delta_assign_inprogress(self):
        item = 'delta_assign_inprogress'
        self.update_sub_dict(item, '1', str(self.sub_deltas_df.filter(pl.col('Date_InProgress').is_between(self.current_date-datetime.timedelta(days=1),
                                                                   self.current_date,
                                                                   closed = 'both')).mean()['Delta_Assign_InProgress'][0]))
        self.update_sub_dict(item, '2', str(self.sub_deltas_df.filter(pl.col('Date_InProgress').is_between(self.current_date-datetime.timedelta(days=90),
                                                                   self.current_date,
                                                                   closed = 'both')).mean()['Delta_Assign_InProgress'][0]))
        self.update_sub_dict(item, '3', str(self.sub_deltas_df.mean()['Delta_Assign_InProgress'][0]))

    def delta_inprogress_closed(self):
        item = 'delta_inprogress_closed'

    def delta_new_closed(self):
        item = 'delta_new_closed'