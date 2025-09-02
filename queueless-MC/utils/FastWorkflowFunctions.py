import polars as pl
import numpy as np
import datetime

class FastWorkflowFunctions:
    def __init__(self, sub_dict, control_type, sub_deltas_df, incoming_dict, outgoing_dict, timedeltas_dict):
        self.sub_dict = sub_dict
        self.control_type = control_type
        self.sub_deltas_df = sub_deltas_df
        self.incoming_dict = incoming_dict
        self.outgoing_dict = outgoing_dict
        self.timedeltas_dict = timedeltas_dict
        self.current_date = datetime.date(2025, 6, 20) # to be replaced with .today() for real data
    
    def update_sub_dict_defects(self, item, time_span, mean):
        try: 
            if mean < 1:
                mean = 'Fewer than 1'
            elif (mean > 1) and (mean < 10):
                mean = 'Between 1 and 10'
            elif (mean > 10) and (mean < 100):
                mean = 'Between 10 and 100'
            else:
                mean = 'More than 100'
        except TypeError:
            mean = 'None'
        self.sub_dict[f'{item}_{self.control_type}_{time_span}'] = {'item': f'{item}_{self.control_type}_{time_span}',
                                                                    'control_type': self.control_type,
                                                                    'time_span': time_span,
                                                                    'mean': mean}
    def update_sub_dict_deltas(self, item, time_span, mean):
        # try: 
        #     mean = str(round(mean), 2)
        # except TypeError:
        #     mean = 'None'
        self.sub_dict[f'{item}_{self.control_type}_{time_span}'] = {'item': f'{item}_{self.control_type}_{time_span}',
                                                                    'control_type': self.control_type,
                                                                    'time_span': time_span,
                                                                    'mean': str(mean)}

    def run_fastworkflow_functions(self):
        self.generated_per_day()
        self.remediated_per_day()
        self.state_deltas()
        return self.sub_dict

    def generated_per_day(self):
        item = 'generated_per_day'
        self.update_sub_dict_defects(item, 'day', np.mean([np.mean(self.incoming_dict[self.control_type][key.strftime('%Y-%m-%d')])
                                                           for key in pl.date_range(self.current_date-datetime.timedelta(days=1),self.current_date,datetime.timedelta(days=1),eager=True)
                                                           if key.strftime('%Y-%m-%d') in self.incoming_dict[self.control_type]]))
        self.update_sub_dict_defects(item, 'month', np.mean([np.mean(self.incoming_dict[self.control_type][key])
                                                           for key in pl.date_range(self.current_date-datetime.timedelta(days=30),self.current_date,datetime.timedelta(days=1),eager=True)
                                                           if key in self.incoming_dict[self.control_type]]))
        self.update_sub_dict_defects(item, 'all_time', np.mean([np.mean(self.incoming_dict[self.control_type][key])
                                                           for key in self.incoming_dict[self.control_type].keys()]))

    def remediated_per_day(self):
        item = 'remediated_per_day'
        self.update_sub_dict_defects(item, 'day', np.mean([np.mean(self.outgoing_dict[self.control_type][key])
                                                           for key in pl.date_range(self.current_date-datetime.timedelta(days=1),self.current_date,datetime.timedelta(days=1),eager=True)
                                                           if key in self.outgoing_dict[self.control_type]]))
        self.update_sub_dict_defects(item, 'month', np.mean([np.mean(self.outgoing_dict[self.control_type][key])
                                                           for key in pl.date_range(self.current_date-datetime.timedelta(days=30),self.current_date,datetime.timedelta(days=1),eager=True)
                                                           if key in self.outgoing_dict[self.control_type]]))
        self.update_sub_dict_defects(item, 'all_time', np.mean([np.mean(self.outgoing_dict[self.control_type][key])
                                                           for key in self.outgoing_dict[self.control_type].keys()]))

    def state_deltas(self):
        items = ['delta_new_assign', 'delta_assign_inprogress', 'delta_inprogress_closed', 'delta_new_closed']
        for i, item in enumerate(items):
            # 1 day ago
            self.update_sub_dict_deltas(item, 'day', self.timedeltas_dict[self.control_type][items[i]][(self.current_date-datetime.timedelta(days=1)).strftime('%Y-%m-%d')])
            # 30 days ago
            indx = list(self.timedeltas_dict[self.control_type][items[i]].keys()).index((self.current_date-datetime.timedelta(days=30)).strftime('%Y-%m-%d'))
            value = np.round(np.mean(list(self.timedeltas_dict[self.control_type][items[i]].values())[indx:]), 2)
            self.update_sub_dict_deltas(item, 'month', value)
            # all time
            self.update_sub_dict_deltas(item, 'all_time', np.round(np.mean(list(self.timedeltas_dict[self.control_type][items[i]].values())), 2))