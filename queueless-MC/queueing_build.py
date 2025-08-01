import pickle
import json

def queueing_build(control_types, empirical_dict):
    generation_distributions = {control_type: empirical_dict[control_type]['incoming_per_hour']
                                for control_type in control_types}
    remediation_distributions = {control_type: empirical_dict[control_type]['delta_new_closed']
                                for control_type in control_types}
    with open('simulations/generation_distributions.json', 'w') as file:
                json.dump(generation_distributions, file)
    with open('simulations/remediation_distributions.json', 'w') as file:
                json.dump(remediation_distributions, file)