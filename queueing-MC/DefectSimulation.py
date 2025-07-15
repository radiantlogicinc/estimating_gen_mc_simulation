import numpy as np
import heapq as hq
import queue
import contextlib
from scipy.stats import skewnorm


class DefectRemediationSimulator:
    def __init__(self, defect_types,
                 defect_priority, 
                 maxValue_incoming, 
                 maxValue_outgoing,
                 skewness_incoming,
                 skewness_outgoing,
                 initial_backlogs,
                 t_end,
                 resources,
                 resources_qmax):
        self.defect_types = defect_types
        if len(defect_priority) == len(defect_types):
            defect_priority_dict = {name: defect_priority[index] for index, name in enumerate(defect_types)}
            self.defect_priority = defect_priority_dict
        else:
            raise ValueError("defect_priority: Must have one value for each defect type")
        
        if len(maxValue_incoming) == len(defect_types):
            maxValue_incoming_dict = {name: maxValue_incoming[index] for index, name in enumerate(defect_types)}
            self.maxValue_incoming = maxValue_incoming_dict
        else:
            raise ValueError("maxValue_incoming: Must have one value for each defect type")
        
        if len(maxValue_outgoing) == len(defect_types):
            maxValue_outgoing_dict = {name: maxValue_outgoing[index] for index, name in enumerate(defect_types)}
            self.maxValue_outgoing = maxValue_outgoing_dict
        else:
            raise ValueError("maxValue_outgoing: Must have one value for each defect type")
        
        if len(skewness_incoming) == len(defect_types):
            skewness_incoming_dict = {name: skewness_incoming[index] for index, name in enumerate(defect_types)}
            self.skewness_incoming = skewness_incoming_dict
        else:
            raise ValueError("skewness_incoming: Must have one value for each defect type")
        
        if len(skewness_outgoing) == len(defect_types):
            skewness_outgoing_dict = {name: skewness_outgoing[index] for index, name in enumerate(defect_types)}
            self.skewness_outgoing = skewness_outgoing_dict
        else:
            raise ValueError("skewness_outgoing: Must have one value for each defect type")
        
        if len(initial_backlogs) == len(defect_types):
            initial_dict = {name: initial_backlogs[index] for index, name in enumerate(defect_types)}
            self.initial_backlogs = initial_dict
        else:
            raise ValueError("initial_backlogs: Must have one value for each defect type")

        self.t_end = t_end
        self.resources = resources
        self.resources_qmax = resources_qmax

    def generate_type_dict(self):
        defect_type_dict = {}
        for name in self.defect_types:
            defect_type_dict[name] = {'priority': self.defect_priority[name],
                                      'skewness_outgoing': self.skewness_outgoing[name], 
                                      'skewness_incoming': self.skewness_incoming[name], 
                                      'maxValue_incoming': self.maxValue_incoming[name], 
                                      'maxValue_outgoing': self.maxValue_outgoing[name],
                                      'initial': self.initial_backlogs[name]}
            

        self.defect_type_dict = defect_type_dict
        return defect_type_dict ###### returned for visualization purposes - is this necessary?
    
    def generate_distributions(self, size=1000):
        # distributions = []
        incoming_distributions = {}
        outgoing_distributions = {}
        for name in self.defect_types:
            #### "Incoming distribution" samples incoming defects/hr ####
            maxValue= self.maxValue_incoming[name]
            samples_incoming = skewnorm.rvs(self.skewness_incoming[name], size=size)
            samples_incoming_pos = samples_incoming - min(samples_incoming)              # positive values only   
            samples_incoming_pos = samples_incoming_pos / max(samples_incoming_pos)      # standadize all the values between 0 and 1 
            samples_incoming_pos = samples_incoming_pos * maxValue                       # spread the standardized values over the range defined by "maxValue"

            #### "Outgoing distribution" samples remediation time [hrs] #### 
            maxValue_outgoing = self.maxValue_outgoing[name]
            samples_outgoing = skewnorm.rvs(self.skewness_outgoing[name], size=size)
            samples_outgoing_pos = samples_outgoing - min(samples_outgoing)           
            samples_outgoing_pos = samples_outgoing_pos / max(samples_outgoing_pos)
            samples_outgoing_pos = samples_outgoing_pos * maxValue_outgoing
            # samples_outgoing_pos = [item + abs(min(samples_outgoing)) for item in samples_outgoing]
            
            incoming_distributions[name] = np.round(samples_incoming_pos) # rounds to nearest integer, for values exactly halfway between, rounds to the nearest even value, e.g. 1.5 and 2.5 round to 2.0, -0.5 and 0.5 round to 0.0
            outgoing_distributions[name] = samples_outgoing_pos

        self.generation_distributions = incoming_distributions
        self.remediation_distributions = outgoing_distributions
        return incoming_distributions, outgoing_distributions ###### returned for visualization purposes - is this necessary?
    
    def compute_time_step(self):
        minvals = []
        for defect_type in self.defect_type_dict.keys():
            temp_list = self.remediation_distributions[defect_type].copy()
            try:
                min_sample = np.min(temp_list[np.nonzero(temp_list)])
                if min_sample < 1: # min sample must be less than the defect generation time step = 1 hr
                    minvals.append(min_sample)
                else:
                    minvals.append(1)
            except ValueError:
                minvals.append(1)   
        dt = min(minvals)/2
        return dt
    
    def load_initial_state(self, initial_state, defect_log, backlog_queue):
        t_start = initial_state['t_end']
        t_end = t_start + self.t_end
        defect_log = initial_state['defect_log']
        for key in defect_log.keys():
            try:
                defect_log[key]['processing_end_time'] ### defect in initial_state has been remediated, it does not get added to the backlog_queue
            except KeyError:
                ### defect has not been remediated yet, add it to the backlog_queue
                hq.heappush(backlog_queue, (self.defect_type_dict[defect_log[key]['defect_type']]['priority'], key)) # tuple (priority level, defect ID tag) will be sorted in heap based on priority
        return t_start, t_end, defect_log, backlog_queue
    
    def initialize_backlog(self, t_start, defect_log, backlog_queue):
        #### INITIALIZATION OF THE BACKLOG ####
        for key in self.defect_type_dict.keys():
            for i in range(self.defect_type_dict[key]['initial']):
                defect_ID = 'ID_' + str(np.random.randint(1,1000000000))
                remediation_time = np.random.choice(self.remediation_distributions[key], size=1, replace=True)
                defect_log[defect_ID] = {'defect_type': key, 't_created': t_start, 'remediation_time': remediation_time}
                hq.heappush(backlog_queue, (self.defect_type_dict[key]['priority'], defect_ID)) # tuple (priority level, defect ID tag) will be sorted in heap based on priority
        return defect_log, backlog_queue
    
    def initialize_queues(self, t_start, defect_log, backlog_queue):
        queue_dict = {} 
        #### INITALIZATION OF THE REMEDIATION QUEUES ####
        for n in range(1, self.resources + 1):
            queue_dict['processing_queue{0}'.format(n)] = queue.Queue(maxsize=self.resources_qmax)
            for _ in range(self.resources_qmax):
                with contextlib.suppress(IndexError):
                    if defect_pull := hq.heappop(backlog_queue)[1]:
                        queue_dict['processing_queue{0}'.format(n)].put(defect_pull, block=False)
                        try:
                            check_var = defect_log[defect_pull]['processing_start_time'] # remediation was already begun in a previous state, continue from where it left off
                        except KeyError:
                            # remediation has not started, start it from now
                            defect_log[defect_pull]['processing_start_time'] = t_start + 0
                        defect_log[defect_pull]['processing_queue'] = 'processing_queue{0}'.format(n)
        return queue_dict, defect_log, backlog_queue
    
    def incoming_defects(self, t, incoming_defects_tracker, incoming_defects_stored, defect_log, backlog_queue):
        for key in incoming_defects_tracker.keys():
            incoming_defects_stored[key].append(incoming_defects_tracker[key][0])
            defect_ID = ['ID_' + str(np.random.randint(1,1000000000)) for _ in range(int(incoming_defects_tracker[key][0]))] ### one defect ID per defect that came in in that hour
            priority_level = [self.defect_type_dict[key]['priority'] for _ in range(int(incoming_defects_tracker[key][0]))]
            remediation_time = [np.random.choice(self.remediation_distributions[key], size=1, replace=True) for _ in range(int(incoming_defects_tracker[key][0]))]
            defect_log.update({defect_ID[i]: {'defect_type': key, 't_created': t, 'remediation_time': remediation_time[i]} for i in range(int(incoming_defects_tracker[key][0]))})
            backlog_queue = backlog_queue + list(zip(priority_level, defect_ID))
            hq.heapify(backlog_queue)
            incoming_defects_tracker[key] = np.random.choice(self.generation_distributions[key], size=1, replace=True) ### reinitialize the incoming_defects_tracker
        return incoming_defects_tracker, incoming_defects_stored, defect_log, backlog_queue
    
    def check_queues(self, n, t, queue_dict, defect_log, backlog_queue):
        if queue_dict['processing_queue{0}'.format(n)].qsize() < self.resources_qmax:
            for i in range(self.resources_qmax - queue_dict['processing_queue{0}'.format(n)].qsize()):
                with contextlib.suppress(IndexError):
                    if defect_pull := hq.heappop(backlog_queue)[1]:
                        queue_dict['processing_queue{0}'.format(n)].put(defect_pull, block=False)
                        defect_log[defect_pull]['processing_start_time'] = t
                        defect_log[defect_pull]['processing_queue'] = 'processing_queue{0}'.format(n)
        return queue_dict, defect_log, backlog_queue 

    def check_remediation(self, n, t, queue_dict, defect_log, backlog_queue):
        for _ in range(self.resources_qmax):
            with contextlib.suppress(queue.Empty):
                if processing_defect := queue_dict['processing_queue{0}'.format(n)].get(block=False): #####
                    if (t - defect_log[processing_defect]['processing_start_time'] >= defect_log[processing_defect]['remediation_time']) and (defect_log[processing_defect]['remediation_time'] != 0):
                        ### defect has been remediated, updated processing_end_time + pick a new defect from the backlog queue to treat
                        leftover_outgoing = (t - defect_log[processing_defect]['processing_start_time']) - defect_log[processing_defect]['remediation_time']
                        defect_log[processing_defect]['processing_end_time'] = t - leftover_outgoing
                        with contextlib.suppress(IndexError):
                            if defect_pull := hq.heappop(backlog_queue)[1]:
                                queue_dict['processing_queue{0}'.format(n)].put(defect_pull, block=False)
                                defect_log[defect_pull]['processing_start_time'] = t - leftover_outgoing
                                defect_log[defect_pull]['processing_queue'] = 'processing_queue{0}'.format(n)
                    else:
                        ### defect has not finished being remediated, put the processing_defect back into the queue
                        queue_dict['processing_queue{0}'.format(n)].put(processing_defect, block=False)
        return queue_dict, defect_log, backlog_queue



    def simulate_defect_backlog(self, dt, initial_state):
        t_start = 0 # assume that starting time of the simulation is now to initialize
        t_end = self.t_end
        defect_log = {}
        backlog_queue = []

        #### IF AVAILABLE, LOAD IN INITIAL STATE ####
        if initial_state:
            t_start, t_end, defect_log, backlog_queue = self.load_initial_state(initial_state, defect_log, backlog_queue)
            
        times1 = np.arange(t_start, t_end, dt) # time step array
        times2 = np.arange(t_start+1, t_end+1) # "on the hour" array (for defect generation)
        times = np.sort(np.concatenate((times1, times2), axis=0))

        
        #### INITIALIZATION OF THE BACKLOG + REMEDIATION PROCESSING QUEUES ####
        defect_log, backlog_queue = self.initialize_backlog(t_start, defect_log, backlog_queue)
        queue_dict, defect_log, backlog_queue = self.initialize_queues(t_start, defect_log, backlog_queue)

        #### INITIALIZATION OF INCOMING DEFECT TRACKER ####
        incoming_defects_tracker = {key: np.random.choice(self.generation_distributions[key], size=1, replace=True) for key in self.defect_type_dict.keys()}
        incoming_defects_stored = {key: [] for key in self.defect_type_dict.keys()}
        hour = t_start + 1.0

        
        #### LOOP OVER TIME ####
        for t in times:
            #### INCOMING "ON THE HOUR" DEFECTS ####
            if t == hour:
                incoming_defects_tracker, incoming_defects_stored, defect_log, backlog_queue = self.incoming_defects(t, 
                                                                                                                     incoming_defects_tracker,
                                                                                                                     incoming_defects_stored,
                                                                                                                     defect_log,
                                                                                                                     backlog_queue)
                hour += 1            
            
            #### UPDATE REMEDIATION ####
            for n in range(1, self.resources + 1):
                ### first check if processing_queue{n} is empty or not filled to resources_qmax
                queue_dict, defect_log, backlog_queue = self.check_queues(n, t, queue_dict, defect_log, backlog_queue)
                ### next check if each defect in processing_queue{n} has been remediated at time t
                queue_dict, defect_log, backlog_queue = self.check_remediation(n, t, queue_dict, defect_log, backlog_queue)

        return np.array(times), incoming_defects_stored, defect_log, backlog_queue