import polars as pl
import time
import json
import pickle
import argparse
from initialize_simulation_ql import initialize_simulation
from delta_table_simulation import delta_table_simulation
from fastworkflow_build import fastworkflow_build
from queueing_build import queueing_build



parser = argparse.ArgumentParser(exit_on_error=False)
### TO DO: error if path_logs not provided
parser.add_argument('--path_logs', type=str, help="Path to the log information (example: 'initial_state_path.pkl')")
# parser.add_argument('--path_empirical_dict', type=str, default=None, help="Path to an existing empirical_dict (example: 'initial_state_path.pkl')")
# parser.add_argument('--path_incoming_dict', type=str, default=None, help="Path to an existing incoming_dict (example: 'initial_state_path.pkl')")
# parser.add_argument('--path_outgoing_dict', type=str, default=None, help="Path to an existing outgoing_dict (example: 'initial_state_path.pkl')")
# parser.add_argument('--path_deltas_df', type=str, default=None, help="Path to an existing deltas_df (example: 'initial_state_path.pkl')")
# parser.add_argument('--path_fastworkflow_df', type=str, default=None, help="Path to an existing fastworkflow_df (example: 'initial_state_path.pkl')")


if __name__ == "__main__":
    args = parser.parse_args()
    # initialize_simulation_ql(args)
    pl.Config.set_tbl_hide_dataframe_shape(True)

    log_df, deltas_df, empirical_dict, incoming_dict, outgoing_dict, timedeltas_dict, control_types = initialize_simulation(args)
    for control_type in control_types:
        deltas_df, empirical_dict, incoming_dict, outgoing_dict, timedeltas_dict = delta_table_simulation(control_type, log_df, deltas_df, empirical_dict, incoming_dict, outgoing_dict, timedeltas_dict)
    
    # fastworkflow_build(control_types, deltas_df, incoming_dict, outgoing_dict, timedeltas_dict)
    queueing_build(control_types, empirical_dict)
    
    # visualize incoming / outgoing distributions
    print(deltas_df)

    # export states as pickled json or csv
    for item in ['empirical_dict', 'incoming_dict', 'outgoing_dict', 'timedeltas_dict', 'deltas_df']:
        if item != 'deltas_df':
            with open(f'simulations/dicts/{item}.json', 'w') as file:
                json.dump(eval(item), file)
        else:
            with open('simulations/deltas_df.json', 'wb') as f:
                deltas_df.sort('Defect_ID').write_json(f)