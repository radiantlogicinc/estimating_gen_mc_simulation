import numpy as np

class VisualizationFunctions:
    def __init__(self):
        pass

    def visualize_histograms_theory(self, fig, axs, index, data, label=None, title=None, color='black'):
        csfont = {'fontname':'Arial'}
        data = np.array(data)
        unique_values = np.unique(data)
        d = 1 if np.all(unique_values) == 0 else np.diff(unique_values).min()
        left_of_first_bin = data.min() - float(d)/2
        right_of_last_bin = data.max() + float(d)/2

        axs[index].hist(data,
                        np.arange(left_of_first_bin, right_of_last_bin + d, d),
                        label=label,
                        histtype='step',
                        linewidth=1.5,
                        density=True,
                        color=color)
        if title:
            axs[index].set_title(title, loc='left', fontsize=12, **csfont)
        return fig, axs
    
    def visualize_histograms_samples(self):
        pass

    def visualize_boxplots(self):
        pass

    def visualize_timeline(self, fig, axs, index, xdata, ydata, ylabel=None, title=None):
        axs[index].plot(xdata, ydata, 'k-o', markersize=6)
        if ylabel:
            axs[index].set_ylabel(ylabel)
        if title:
            axs[index].set_title(title, loc='left', fontsize=10)
        return fig, axs