# import pandas as pd
# import numpy as np
import matplotlib.pyplot as plt
import argparse

#### INPUT PARAMETERS ####
parser = argparse.ArgumentParser(exit_on_error=False)
parser.add_argument('--t_end', type=float, help='End time for remediation simulation, in hours')

if __name__ == "__main__":
    ####### INITIALIZATION #######
    args = parser.parse_args()
    ## initialization

    ## import log data from csv
    logs_df = pd.read_csv('defectRemediation_log_simulation_vPrototype.csv', index_col='ID')
    logs_df['Timestamp'] = pd.to_datetime(logs_df['Timestamp'], format='%Y-%d-%m %H:%M:%S')
    
    ## add new column to logs_df to track hour of timestamp (for incoming per hour defects)
    hours = []
    for row in logs_df.iterrows():
        hours.append(logs_df.loc[row[0]]['Timestamp'].hour)
    logs_df['Hour'] = hours

    ## Build incoming defects distributions from logs_df, stored in empirical_dict --> 'incoming_per_hour'
    empirical_dict = {}
    incoming_dict = {}
    control_types = logs_df['Control_Type'].unique()
    for control_type in control_types:
        empirical_dict[control_type] = {'incoming_per_hour': [],
                                        'delta_new_assign': [],
                                        'delta_assign_inprogress': [],
                                        'delta_inprogress_closed': []
                                    }
        incoming_dict[control_type] = {}

    for control_type in control_types:
        sub_logs_df = logs_df.query('(Control_Type == @control_type) and (State == "new")')
        for day in logs_df["Date"].unique():
            sub_logs_df_date = sub_logs_df.query('Date == @day')['Hour'].value_counts()
            incoming_defects = [sub_logs_df_date.loc[i] if i in sub_logs_df_date.index else 0 for i in range(0,24)]
            incoming_dict[control_type].update({day: incoming_defects})
            empirical_dict[control_type]['incoming_per_hour'] = empirical_dict[control_type]['incoming_per_hour'] + incoming_dict[control_type][day]

    ## Creating empirical Delta table to track time b/w state changes
    deltas_df = pd.DataFrame(columns=['Defect_ID','Control_Type','Delta_New_Assign','Delta_Assign_InProgress','Delta_InProgress_Closed'])
    ######### TO DO ##########
    # check if deltas_df exists before initializing empty Delta table

    try: # deltas_df contains some entries, add onto it
        Last_ID_processed = deltas_df.index[-1]
        index = Last_ID_processed + 1
    except IndexError: # deltas_df is empty, start from beginning
        index = 0 

    for defect_id in logs_df['Defect_ID'].unique():
        #### Recover timestamps for each state change ####
        timestamp_new = logs_df.loc[(logs_df['Defect_ID']==defect_id) & (logs_df['State']=='new')]['Timestamp'].iloc[0] # OR logs_df.query('(Defect_ID == defect_id) and (State == "new")')['Timestamp'].iloc[0]
        timestamp_assign = logs_df.loc[(logs_df['Defect_ID']==defect_id) & (logs_df['State']=='assign')]['Timestamp'].iloc[0]
        timestamp_inprogress = logs_df.loc[(logs_df['Defect_ID']==defect_id) & (logs_df['State']=='in-progress')]['Timestamp'].iloc[0]
        timestamp_closed = logs_df.loc[(logs_df['Defect_ID']==defect_id) & (logs_df['State']=='closed')]['Timestamp'].iloc[0]

        # time between assign and new (hrs)
        delta_new_assign = timestamp_assign - timestamp_new
        delta_new_assign = round(delta_new_assign.total_seconds()/3600, 3)
        
        # time between in-progress and assign (hrs)
        delta_assign_inprogress = timestamp_inprogress - timestamp_assign
        delta_assign_inprogress = round(delta_assign_inprogress.total_seconds()/3600, 3)
        
        # time between closed and in-progress (hrs)
        delta_inprogress_closed = timestamp_closed - timestamp_inprogress
        delta_inprogress_closed = round(delta_inprogress_closed.total_seconds()/3600, 3)

        # append row to df and iterate index
        control_type = logs_df.loc[logs_df['Defect_ID'] == defect_id].iloc[0]['Control_Type']
        deltas_df.loc[index] = [defect_id, control_type, delta_new_assign, delta_assign_inprogress, delta_inprogress_closed]
        index += 1

    ## Propagate delta values into empirical_dict --> 'delta_new_assign', 'delta_assign_inprogress', 'delta_inprogress_closed'
    for control_type in control_types:
        empirical_dict[control_type]['delta_new_assign'].extend(deltas_df.query('Control_Type == @control_type')['Delta_New_Assign'].to_list())
        empirical_dict[control_type]['delta_assign_inprogress'].extend(deltas_df.query('Control_Type == @control_type')['Delta_Assign_InProgress'].to_list())
        empirical_dict[control_type]['delta_inprogress_closed'].extend(deltas_df.query('Control_Type == @control_type')['Delta_InProgress_Closed'].to_list())


    ####### VISUALIZATION #######
    ## Create histograms from empirical data
    csfont = {'fontname':'Arial'}
    fig, axs = plt.subplots (len(empirical_dict.keys()), 4, figsize=(16,20))
    for index, value in enumerate(empirical_dict.keys()):
        data = np.array(empirical_dict[value]['incoming_per_hour'])
        unique_values = np.unique(data)
        if np.all(unique_values) == 0:
            d = 1
        else:
            d = np.diff(unique_values).min()
        left_of_first_bin = data.min() - float(d)/2
        right_of_last_bin = data.max() + float(d)/2
        axs[index][0].hist(data, np.arange(left_of_first_bin, right_of_last_bin + d, d), density=True)
        axs[index][0].set_title(f'{value} - incoming per hour', loc='left', fontsize=14, **csfont)
        axs[index][1].hist(empirical_dict[value]['delta_new_assign'], density=True)
        axs[index][2].hist(empirical_dict[value]['delta_assign_inprogress'], density=True)
        axs[index][3].hist(empirical_dict[value]['delta_inprogress_closed'], density=True)

        axs[index][1].set_title('Delta_New_Assign', loc='left', fontsize=12, **csfont)
        axs[index][2].set_title('Delta_Assign_InProgress', loc='left', fontsize=12, **csfont)
        axs[index][3].set_title('Delta_InProgress_Closed', loc='left', fontsize=12, **csfont)

    plt.show()

    ####### BACKLOG SIMULATION #######
    # Simulate Delta table based on empirical histograms
    projection_dict = {}
    t_end = 50 # duration of the simulation, in (hrs)
    for control_type in empirical_dict.keys():
        #### first sample how many defects were generated in t_end from empirical data ####
        sampled_values_incoming = np.random.choice(empirical_dict[control_type]['incoming_per_hour'], size=t_end, replace=True)
        nonzero_incoming_indices = np.nonzero(sampled_values_incoming) # indices of nonzero values in sampled_values_incoming
        nonzero_values_count = nonzero_incoming_indices[0].size

        #### assign a defect_ID to each generated defect ####
        defect_IDs = np.random.randint(1,1000000000, size=nonzero_values_count)
        defect_IDs = ['ID_' + str(defect_IDs[i]) for i in range(nonzero_values_count)] 

        #### sample time deltas for each generated defect from empirical data ####
        sampled_delta_new_assign = np.random.choice(empirical_dict[control_type]['delta_new_assign'], size=nonzero_values_count, replace=True)
        sampled_delta_assign_inprogress = np.random.choice(empirical_dict[control_type]['delta_new_assign'], size=nonzero_values_count, replace=True)
        sampled_delta_inprogress_closed = np.random.choice(empirical_dict[control_type]['delta_new_assign'], size=nonzero_values_count, replace=True)

        #### add generated defects to dictionary - to be turned into pandas df ####
        for i in range(nonzero_values_count):
            projection_dict[defect_IDs[i]] = {'Control_Type': control_type,
                                            'Delta_New_Assign': sampled_delta_new_assign[i],
                                            'Delta_Assign_InProgress': sampled_delta_assign_inprogress[i],
                                            'Delta_InProgress_Closed': sampled_delta_inprogress_closed[i]}

    deltas_df_simulated = pd.DataFrame.from_dict(projection_dict, orient='index')



