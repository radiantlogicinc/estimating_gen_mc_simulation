import pickle

def queueing_build(control_types, empirical_dict):
    generation_distributions = {control_type: empirical_dict[control_type]['incoming_per_hour']
                                for control_type in control_types}
    remediation_distributions = {control_type: empirical_dict[control_type]['delta_new_closed']
                                for control_type in control_types}
    with open('simulations/generation_distributions.pkl', 'wb') as file:
                pickle.dump(generation_distributions, file)
    with open('simulations/remediation_distributions.pkl', 'wb') as file:
                pickle.dump(remediation_distributions, file)