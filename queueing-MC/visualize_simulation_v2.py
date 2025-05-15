import matplotlib.pyplot as plt
from itertools import count
import numpy as np


def visualize_simulation(comparison_dict):
    #### VISUALIZATION ####
    defects_created = {}
    defects_remediated = {}
    list_remediated = {}
    list_remediated_forplot = {}
    times = {}
    times_forplot = {}
    backlog = {}

    #### Reconstructing the backlog - defects generated and remediated ####
    for trial in comparison_dict.keys():
        t_end = int(comparison_dict[trial]['t_end'])
        hours = list(range(t_end+1))
        initial_backlog = sum([1 for key in comparison_dict[trial]['defect_log'].keys() if comparison_dict[trial]['defect_log'][key]['t_created'] == 0])
        defects_created[trial] = {}
        defects_remediated[trial] = {}
        for key in comparison_dict[trial]['defect_log'].keys():
            defects_created[trial][key] = comparison_dict[trial]['defect_log'][key]['t_created']
            try:
                defects_remediated[trial][key] = comparison_dict[trial]['defect_log'][key]['processing_end_time']
            except KeyError:
                pass

        list_remediated[trial] = [defects_remediated[trial][key] for key in defects_remediated[trial].keys()]
        list_remediated_forplot[trial] = [defects_remediated[trial][key][0] for key in defects_remediated[trial].keys()] # remove the np.ndarray for the values to be plotted
        times[trial] = sorted(list(range(t_end+1)) + list_remediated[trial]) # keep np.ndarray to distinguish between "on the hour" generated and remediated
        times_forplot[trial] = sorted(list(range(t_end+1)) + list_remediated_forplot[trial])
        backlog[trial] = []

        niter = count(0)
        for t in times[trial]:
            if t == 0:
                backlog[trial].append(initial_backlog)
            elif type(t) == np.ndarray:
                value = backlog[trial][next(niter)] - 1
                backlog[trial].append(value)
            elif t in list(defects_created[trial].values()):
                res = sum(int(value) == t for value in defects_created[trial].values())
                value = backlog[trial][next(niter)] + res
                backlog[trial].append(value)
            else:
                backlog[trial].append(backlog[trial][next(niter)])

    backlog_max = max(i for v in backlog.values() for i in v)
    backlog_min = min(i for v in backlog.values() for i in v)

    visualize_boxplot(hours, times_forplot, backlog)

    
def visualize_boxplot(hours, times_forplot, backlog):
    #### Constructing and visualizing the box plots per unit of time ####
    hourly_stats = {}
    median_curve = []
    min_curve = []
    max_curve = []

    for hour in hours[:-1]:
        hourly_stats['hour{0}'.format(hour+1)] = []
        for trial in times_forplot.keys():
            starting = times_forplot[trial].index(hours[hour])
            stopping = times_forplot[trial].index(hours[hour+1])
            hourly_stats['hour{0}'.format(hour+1)] = hourly_stats['hour{0}'.format(hour+1)] + backlog[trial][starting:stopping]
        median_curve.append(np.percentile(hourly_stats['hour{0}'.format(hour+1)],[50])[0])
        min_curve.append(min(hourly_stats['hour{0}'.format(hour+1)]))
        max_curve.append(max(hourly_stats['hour{0}'.format(hour+1)]))

    #### "Clean" = backlog < 20% of initial    
    colors = ['#2348FF' if value < max(median_curve)*0.2 else '#C21445' for value in median_curve]

    fig, ax = plt.subplots(1, 1, figsize=(16,5))

    bplot = ax.boxplot(list(hourly_stats.values()),
                       labels=hours[1:],
                       patch_artist=True,
                       sym='',
                       notch=False) # each box plot at time_step corresponds to the averaged data from [time_step-1, time_step)
    for patch, color in zip(bplot['boxes'], colors):
        patch.set_facecolor(color)

    for trial in times_forplot.keys():
        ax.plot(times_forplot[trial], backlog[trial], ':', linewidth=0.5, color='navy', alpha=0.5) # plotting raw signals
    
    csfont = {'fontname':'Arial'}
    # ax.plot(hours[1:], max_curve, 'r', linewidth=0.75, alpha=0.3)
    ax.plot(hours[1:], median_curve, 'k', linewidth=1, alpha=0.5, label='median "on the hour"')
    # ax.plot(hours[1:], min_curve, 'b', linewidth=0.75, alpha=0.3)
    # ax.fill_between(hours[1:], min_curve, max_curve, color='purple', alpha=0.1)
    ax.set_xticks(hours[::10], labels=hours[::10])
    ax.set_xlim(0, max(times_forplot[trial]))
    ax.set_xlabel('times (hrs)', fontsize=14, **csfont)
    ax.set_ylabel('defects backlog\n(average/hour)', fontsize=14, **csfont)
    # ax.legend()
    plt.show()


def visualize_generation_distributions(defect_type_dict, incoming_defects_dict, generation_distributions):
    incoming_defects = {key: [] for key in defect_type_dict.keys()}

    for trial in incoming_defects_dict.keys():
        for key in incoming_defects_dict[trial].keys():
            # incoming_defects[key].append(incoming_defects_dict[trial][key])
            incoming_defects[key] = incoming_defects[key] + incoming_defects_dict[trial][key]

    csfont = {'fontname':'Arial'}
    fig, axs = plt.subplots (1, len(defect_type_dict.keys()), figsize=(16,4))
    
    for index, value in enumerate(defect_type_dict.keys()):
        data = np.array(generation_distributions[value])
        unique_values = np.unique(data)
        if np.all(unique_values) == 0:
            d = 1
        else:
            d = np.diff(unique_values).min()
        left_of_first_bin = data.min() - float(d)/2
        right_of_last_bin = data.max() + float(d)/2
        axs[index].hist(incoming_defects[value], np.arange(left_of_first_bin, right_of_last_bin + d, d), label=f'{len(incoming_defects[value])} samples', color='#C21445', alpha=0.2, edgecolor='black', linewidth=1.5, density=True)
        axs[index].hist(data, np.arange(left_of_first_bin, right_of_last_bin + d, d), label=f"theory, α={defect_type_dict[value]['skewness_incoming']}", linewidth=2, color='#C21445', density=True, histtype='step')
        # axs[0].set_title('Type 1', fontsize=10, **tnrfont)
        axs[index].set_title(f'Type {value[-1]}', loc='left', fontsize=14, **csfont)
        axs[index].legend(loc='upper right', bbox_to_anchor=(1.02, 1.13), fontsize='x-small')
        axs[index].set_xticks(np.unique(data))

    axs[0].set_ylabel('density', fontsize=14, **csfont)
    fig.text(0.5, 0, '# incoming defects per hour', ha='center', fontsize=14, **csfont)
    plt.show()


def visualize_remediation_distributions(defect_type_dict, comparison_dict, remediation_distributions):
    incoming_remediations = {key: [] for key in defect_type_dict.keys()}

    for trial in comparison_dict.keys():
        for key in comparison_dict[trial]['defect_log'].keys():
            incoming_remediations[comparison_dict[trial]['defect_log'][key]['defect_type']].append(comparison_dict[trial]['defect_log'][key]['remediation_time'][0])

    csfont = {'fontname':'Arial'}
    fig, axs = plt.subplots (1, len(defect_type_dict.keys()), figsize=(16,4))
    
    for index, value in enumerate(defect_type_dict.keys()):
        axs[index].hist(incoming_remediations[value], label=f'{len(incoming_remediations[value])} samples', color='#2348FF', alpha=0.2, edgecolor='black', linewidth=1.5, density=True)
        axs[index].hist(remediation_distributions[value], label=f"theory, α={defect_type_dict[value]['skewness_outgoing']}", histtype='step', linewidth=2, color='#2348FF', density=True)
        axs[index].set_title(f'Type {value[-1]}', loc='left', fontsize=14, **csfont)
        axs[index].legend(loc='upper right', bbox_to_anchor=(1.02, 1.13), fontsize='x-small')

    axs[0].set_ylabel('density', fontsize=14, **csfont)
    fig.text(0.5, 0, 'remediation time (hrs)', ha='center', fontsize=14, **csfont)
    plt.show()