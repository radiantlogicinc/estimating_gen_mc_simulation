import numpy as np

class VisualizationFunctions:
    def __init__(self):
        self.csfont = {'fontname':'Arial'}

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
        axs[index].plot(xdata, ydata, 'k-', linewidth=1.5)
        axs[index].plot(xdata, ydata, 'o', linewidth=1.5, color='black', markersize=8)
        axs[index].plot(xdata, ydata, 'o', linewidth=1.5, color='#E24500', markersize=6)
        if ylabel:
            axs[index].set_ylabel(ylabel, fontsize=10, **self.csfont)
        if title:
            axs[index].set_title(title, loc='left', fontsize=10, **self.csfont)
        # ydata_unique = np.unique(ydata, return_index=True)
        # xdata_reduced = list(set([xdata[x] for x in ydata_unique[1]] + xdata[::5]))
        xdata_reduced = xdata[::3]
        axs[index].set_xticks(xdata_reduced)
        axs[index].set_xticklabels(xdata_reduced, rotation=-45, fontsize=8, ha="left", **self.csfont)
        # axs[index].grid(axis='x', color='#8C97C4', alpha=0.5, linestyle='-', linewidth=1)
        return fig, axs