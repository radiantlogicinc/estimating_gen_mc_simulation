import polars as pl
import time
import argparse
from initialize_simulation_ql import initialize_simulation
from delta_table_simulation import delta_table_simulation


parser = argparse.ArgumentParser(exit_on_error=False)
### TO DO: error if path_logs not provided
parser.add_argument('--path_logs', type=str, help="Path to the log information (example: 'initial_state_path.pkl')")
parser.add_argument('--path_empirical_dict', type=str, default=None, help="Path to an existing empirical_dict (example: 'initial_state_path.pkl')")
parser.add_argument('--path_incoming_dict', type=str, default=None, help="Path to an existing incoming_dict (example: 'initial_state_path.pkl')")
parser.add_argument('--path_outgoing_dict', type=str, default=None, help="Path to an existing outgoing_dict (example: 'initial_state_path.pkl')")
parser.add_argument('--path_deltas_df', type=str, default=None, help="Path to an existing deltas_df (example: 'initial_state_path.pkl')")


if __name__ == "__main__":
    args = parser.parse_args()
    # initialize_simulation_ql(args)
    pl.Config.set_tbl_hide_dataframe_shape(True)

    log_df, deltas_df, empirical_dict, incoming_dict, outgoing_dict, control_types = initialize_simulation(args)
    for control_type in control_types:
        deltas_df, empirical_dict, incoming_dict, outgoing_dict = delta_table_simulation(control_type, log_df, deltas_df, empirical_dict, incoming_dict, outgoing_dict)

    print(deltas_df)        


    

    