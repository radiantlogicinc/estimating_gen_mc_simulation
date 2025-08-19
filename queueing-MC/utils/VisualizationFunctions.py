import matplotlib.pyplot as plt
import numpy as np

# class VisualizationFunctions:
def __init__(self):
    self.csfont = {'fontname':'Arial'}

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
            # hourly_stats['hour{0}'.format(hour+1)] = hourly_stats['hour{0}'.format(hour+1)] + backlog[trial][starting:stopping]
            hourly_stats['hour{0}'.format(hour+1)] += backlog[trial][starting:stopping]
        median_curve.append(np.percentile(hourly_stats['hour{0}'.format(hour+1)],[50])[0])
        min_curve.append(min(hourly_stats['hour{0}'.format(hour+1)]))
        max_curve.append(max(hourly_stats['hour{0}'.format(hour+1)]))

    #### "Clean" = backlog < 20% of initial    
    colors = ['#2348FF' if value < max(median_curve)*0.2 else '#C21445' for value in median_curve]

    fig, ax = plt.subplots(1, 1, figsize=(10,4))

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
    if len(defect_type_dict.keys()) > 1:
        fig, axs = plt.subplots (1, len(defect_type_dict.keys()), figsize=(12,4))
    else:
        fig, axs = plt.subplots (1, len(defect_type_dict.keys()), figsize=(8,4))
    
    for index, value in enumerate(defect_type_dict.keys()):
        data = np.array(generation_distributions[value])
        unique_values = np.unique(data)
        # if np.all(unique_values) == 0:
        #     d = 1
        # else:
        #     d = np.diff(unique_values).min()
        d = 1 if np.all(unique_values) == 0 else np.diff(unique_values).min()
        left_of_first_bin = data.min() - float(d)/2
        right_of_last_bin = data.max() + float(d)/2
        if len(defect_type_dict.keys()) > 1:
            axs[index].hist(incoming_defects[value], np.arange(left_of_first_bin, right_of_last_bin + d, d), label=f'{len(incoming_defects[value])} samples', color='#F6B9B9', edgecolor='#969696', linewidth=1.5, density=True)
            axs[index].hist(data, np.arange(left_of_first_bin, right_of_last_bin + d, d), label=f"theory, α={defect_type_dict[value]['skewness_incoming']}", linewidth=2, color='#E95050', density=True, histtype='step')
            # axs[0].set_title('Type 1', fontsize=10, **tnrfont)
            axs[index].set_title(value, loc='left', fontsize=14, **csfont)
            axs[index].legend(loc='upper right', bbox_to_anchor=(1.02, 1.13), fontsize='x-small')
            axs[index].set_xticks(np.unique(data))
        else:
            axs.hist(incoming_defects[value], np.arange(left_of_first_bin, right_of_last_bin + d, d), label=f'{len(incoming_defects[value])} samples', color='#F6B9B9', edgecolor='#969696', linewidth=1.5, density=True)
            axs.hist(data, np.arange(left_of_first_bin, right_of_last_bin + d, d), label=f"theory, α={defect_type_dict[value]['skewness_incoming']}", linewidth=2, color='#E95050', density=True, histtype='step')
            # axs[0].set_title('Type 1', fontsize=10, **tnrfont)
            axs.set_title(value, loc='left', fontsize=14, **csfont)
            axs.legend(loc='upper right', bbox_to_anchor=(1.02, 1.13), fontsize='x-small')
            axs.set_xticks(np.unique(data))
    if len(defect_type_dict.keys()) > 1:
        axs[0].set_ylabel('density', fontsize=14, **csfont)
    else:
        axs.set_ylabel('density', fontsize=14, **csfont)
    fig.text(0.5, 0, '# incoming defects per hour', ha='center', fontsize=14, **csfont)
    plt.show()


def visualize_remediation_distributions(length, defect_type_dict, comparison_dict, remediation_distributions):
    incoming_remediations = {key: [] for key in defect_type_dict.keys()}

    for trial in comparison_dict.keys():
        for key in comparison_dict[trial]['defect_log'].keys():
            incoming_remediations[comparison_dict[trial]['defect_log'][key]['defect_type']].append(comparison_dict[trial]['defect_log'][key]['remediation_time'][0])

    csfont = {'fontname':'Arial'}
    
    if length > 1:
        fig, axs = plt.subplots (1, len(defect_type_dict.keys()), figsize=(12,4))
        for index, value in enumerate(defect_type_dict.keys()):
            axs[index].hist(incoming_remediations[value], label=f'{len(incoming_remediations[value])} samples', color='#D3DAFF', edgecolor='#969696', linewidth=1.5, density=True)
            axs[index].hist(remediation_distributions[value], label=f"theory, α={defect_type_dict[value]['skewness_outgoing']}", histtype='step', linewidth=2, color='#2348FF', density=True)
            axs[index].set_title(value, loc='left', fontsize=14, **csfont)
            axs[index].legend(loc='upper right', bbox_to_anchor=(1.02, 1.13), fontsize='x-small')
    else:
        fig, axs = plt.subplots (1, len(defect_type_dict.keys()), figsize=(8,4))
        for key in defect_type_dict.keys():
            axs.hist(incoming_remediations[key], label=f'{len(incoming_remediations[key])} samples', color='#D3DAFF', edgecolor='#969696', linewidth=1.5, density=True)
            axs.hist(remediation_distributions[key], label=f"theory, α={defect_type_dict[key]['skewness_outgoing']}", histtype='step', linewidth=2, color='#2348FF', density=True)
            axs.set_title(key, loc='left', fontsize=14, **csfont)
            axs.legend(loc='upper right', bbox_to_anchor=(1.02, 1.13), fontsize='x-small')

    if len(defect_type_dict.keys()) > 1:
        axs[0].set_ylabel('density', fontsize=14, **csfont)
    else:
        axs.set_ylabel('density', fontsize=14, **csfont)
    fig.text(0.5, 0, 'remediation time (hrs)', ha='center', fontsize=14, **csfont)
    plt.show()

def visualize_histograms_theory(self, fig, axs, index, data, label=None, title=None, color='black', step=False):
    data = np.array(data)
    unique_values = np.unique(data)
    d = 1 if np.all(unique_values) == 0 else np.diff(unique_values).min()
    left_of_first_bin = data.min() - float(d)/2
    right_of_last_bin = data.max() + float(d)/2

    if step:
        axs[index].hist(data,
                    np.arange(left_of_first_bin, right_of_last_bin + d, d),
                    label=label,
                    linewidth=1.5,
                    density=True,
                    color=color,
                    histtype='step')
    else:
        axs[index].hist(data,
                        np.arange(left_of_first_bin, right_of_last_bin + d, d),
                        label=label,
                        linewidth=1.5,
                        density=True,
                        color=color,
                        alpha=0.5,
                        edgecolor='black')
    if title:
        axs[index].set_title(title, loc='left', fontsize=12, **self.csfont)
    return fig, axs

def visualize_timeline(self, fig, axs, index, xdata, ydata, ylabel=None, title=None):
    axs[index].plot(xdata, ydata, 'k-o', markersize=6)
    if ylabel:
        axs[index].set_ylabel(ylabel, **self.csfont)
    if title:
        axs[index].set_title(title, loc='left', fontsize=10, **self.csfont)
    return fig, axs