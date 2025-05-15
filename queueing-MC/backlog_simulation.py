import argparse
import pickle
import time
# from defectSimulation_v2 import defectRemediationSimulator

def backlog_simulation(args, defect_simulation, dt):
    #### Initialization of remaining variables ####
    trials = args.trials
    if args.check_initial_state == 'True':
        check_initial_state = True
    else:
        check_initial_state = False
    if args.export_final_state == 'True':
        export_final_state = True
    else:
        export_final_state = False


    #### LOAD IN EXISTING STATE ####
    if check_initial_state:
        with open(args.path_initial_state, 'rb') as f:
            loaded_dict = pickle.load(f)
        trials = len(loaded_dict.keys())
        # initial_backlogs = [0, 0, 0, 0, 0]


    ####### BACKLOG SIMULATION ########
    incoming_defects_dict = {}
    comparison_dict = {}
    for i in range(trials):
        start = time.time()
        if check_initial_state:
            initial_state = loaded_dict['trial{0}'.format(i+1)]
        else:
            initial_state = {}
        times, incoming_defects, defect_log, backlog_queue_remaining = defect_simulation.simulate_defect_backlog(dt, initial_state)
        end = time.time()
        incoming_defects_dict['trial{0}'.format(i+1)] = incoming_defects
        if args.t_end != 0:
            comparison_dict['trial{0}'.format(i+1)] = {'simulation_time': end-start, 't_end': times[-1], 'time_step': dt, 'defect_log': defect_log}
        else:
            try:
                comparison_dict['trial{0}'.format(i+1)] = {'simulation_time': end-start, 't_end': initial_state['t_end'], 'time_step': dt, 'defect_log': defect_log}
            except KeyError:
                ### t_end = 0 and initial_state empty, no simulation was done, considers only the initial conditions at t = 0
                comparison_dict['trial{0}'.format(i+1)] = {'simulation_time': end-start, 't_end': 0, 'time_step': dt, 'defect_log': defect_log}
        print(f'Elapsed time (trial {i+1}):', end-start, 'seconds')

    ### SAVE SIMULATION RESULTS AS JSON ####
    if export_final_state:
        with open(args.path_final_state, 'wb') as f:
            pickle.dump(comparison_dict, f)

    return incoming_defects_dict, comparison_dict