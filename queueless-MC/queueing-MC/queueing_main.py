import argparse
from DefectSimulation import DefectRemediationSimulator
from initialize_simulation import initialize_simulation
from backlog_simulation import backlog_simulation
from visualize_simulation import visualize_simulation, visualize_generation_distributions, visualize_remediation_distributions


#### INPUT PARAMETERS ####
parser = argparse.ArgumentParser(exit_on_error=False)
parser.add_argument('--defect_labels', type=str, help="List defect types to simulate seperated by , (example: 'type1, type2, type3')")
parser.add_argument('--defect_priority', type=str, help="List defect priorities (whole numbers) for each defect type in --defect_labels (lower = higher priority) seperated by , (example: '11, 3, 5')")
# parser.add_argument('--maxValue_generation', type=str, help="List max value of incoming defect distribution (whole numbers) for each defect type in --defect_labels seperated by , (example: '1, 2, 3')")
# parser.add_argument('--maxValue_remediation', type=str, help="List max value of remediation time distribution (floating point values) for each defect type in --defect_labels seperated by , (example: '3.5, 4, 5.5')")
# parser.add_argument('--skewness_generation', type=str, help="List skewness of incoming defect distribution for each defect type in --defect_labels seperated by , (example: '')")
# parser.add_argument('--skewness_remediation', type=str, help="List skewness of remediation time distribution for each defect type in --defect_labels seperated by , (example: '')")
parser.add_argument('--initial_backlogs', type=str, help="List initial number of backlogged defects for each defect type in --defect_labels seperated by , (example: '')")
parser.add_argument('--t_end', type=float, help='End time for remediation simulation, in hours')
parser.add_argument('--resources', type=int, help='Available parallel resources for treatment of defects')
parser.add_argument('--resources_qmax', type=int, help='Maxmimum resources that can be alloted at any given time for treatment of defects')
parser.add_argument('--trials', type=int, help='Number of times to run the full simulation')
parser.add_argument('--check_initial_state', default=False, help='If importing an existing simulation to continue, enter True; False otherwise')
parser.add_argument('--path_initial_state', type=str, help="If --check_initial_state is True, provide the path to the existing simulation (example: 'initial_state_path.pkl')")
parser.add_argument('--export_final_state', default=False, help='If exporting current simulation, enter True; False otherwise')
parser.add_argument('--path_final_state', type=str, help="If --export_final_state is True, provide the path to store the simulation (example: 'final_state_path.pkl')")
### add paths for importing + exporting

if __name__ == "__main__":
    ####### INITIALIZATION #######
    args = parser.parse_args()
    defect_simulation, defect_type_dict, generation_distributions, remediation_distributions, dt = initialize_simulation(args)

    ####### BACKLOG SIMULATION #######
    incoming_defects_dict, comparison_dict = backlog_simulation(args, defect_simulation, dt)
    
    ####### VISUALIZATION #######
    visualize_simulation(comparison_dict)                                                                   # defect backlog
    visualize_generation_distributions(defect_type_dict, incoming_defects_dict, generation_distributions)   # number of incoming defects / hour distributions
    visualize_remediation_distributions(defect_type_dict, comparison_dict, remediation_distributions)       # remediation time distributions