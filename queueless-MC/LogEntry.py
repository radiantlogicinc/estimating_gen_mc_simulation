import polars as pl

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