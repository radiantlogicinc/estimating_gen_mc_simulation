from DefectSimulation import DefectRemediationSimulator

def initialize_simulation(args):
    """
    Class instantiation and simulation intialization based on input parameters.

    Args
        :args (argparse parser.parse_args()): parsed input arguments
    Returns
        :defect_simulation (instance of class): instance of class DefectRemediationSimulator
        :defect_type_dict (dict): maps defect types to corresponing poisson rates, skewness and initial_backlogs
        :generation_distributions (dict): defect incidence rate histograms (per defect type)
        :remediation_distributions (dict): defect remediation time histograms (per defect type)
        :dt (float): simulation time step, equivalent to 1/2 the minimum value among histograms (Nyquist sampling theorem)
    """
    #### Initialization variables ####
    defect_labels = args.defect_labels.split(', ')
    defect_priority = list(map(int, args.defect_priority.split(', ')))
    maxValue_generation = list(map(float, args.maxValue_generation.split(', ')))
    maxValue_remediation = list(map(float, args.maxValue_remediation.split(', ')))
    skewness_generation = list(map(int, args.skewness_generation.split(', ')))
    skewness_remediation = list(map(int, args.skewness_remediation.split(', ')))
    initial_backlogs = list(map(int, args.initial_backlogs.split(', ')))
    #### Simulation variables ####
    t_end = args.t_end
    resources = args.resources
    resources_qmax = args.resources_qmax

    ####### INITIALIZATION ########
    #### Simulation instantiation ####
    defect_simulation = DefectRemediationSimulator(defect_labels,
                                                   defect_priority,
                                                   maxValue_generation,
                                                   maxValue_remediation,
                                                   skewness_generation,
                                                   skewness_remediation,
                                                   initial_backlogs,
                                                   t_end,
                                                   resources,
                                                   resources_qmax)

    defect_type_dict = defect_simulation.generate_type_dict() # map defect type to corresponing poisson_rate, skewness and initial_backlogs
    generation_distributions, remediation_distributions = defect_simulation.generate_distributions() # generate remediation time distribution for each defect type
    dt = defect_simulation.compute_time_step()
    ### histogram of distributions
    return defect_simulation, defect_type_dict, generation_distributions, remediation_distributions, dt