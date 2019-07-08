import geopandas
import luigi
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sys import path
from matplotlib.ticker import StrMethodFormatter
from mpl_toolkits.axes_grid1 import make_axes_locatable

# Custom functions and other Luigi tasks
path.insert(0, '/app/src/data_tasks')
path.insert(0, '/app/src')
from custom_functions import make_geodf
from make_summaries import ArsenicSummary
from make_summaries import FluorideSummary
from maps_base import BaseMap


class ArsenicMap(BaseMap):
    """
    Create a chloropleth summary map of arsenic burden in Mexico
    """
    output_file = '../reports/figures/summary_arsenic.png'

    def requires(self):
        yield ArsenicSummary()

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        # Import summary
        as_state_summary = pd.read_csv('processed/summary_arsenic.csv', index_col=0)
        # Import map of all states
        states = geopandas.read_file('interim/states')
        states2 = states[['NAME_1','geometry']] # Reduce columns
        states2.columns = ['state','geometry'] # Rename columns

        # Merge the map of the states with their summary statistics
        as_states = states2.merge(as_state_summary, on='state', how='left')
        as_states = as_states.fillna(0)

        fig = plt.figure(figsize=(40,40))
        self.draw_mexico(fig)
        ax=fig.axes[-1]

        divider = make_axes_locatable(ax)
        cax = divider.append_axes("right", size="3%", pad=0.1)

        as_states.plot(column='cancer_incidence', ax=ax, 
                    legend=True,  cax=cax, cmap='OrRd',
                    edgecolor='black')
        ax.tick_params(
            axis='both', bottom=False, left=False,         
            labelbottom=False, labelleft=False) 

        for l in cax.yaxis.get_ticklabels():
            l.set_fontsize(36)

        cax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
        # ax.set_title('Lifetime cancer cases caused by arsenic', 
        #                     fontsize=60, pad=20)

        print(f'>> Making map of arsenic burden by state...')
        fig.savefig(self.output_file, bbox_inches='tight')


class FluorideMap(BaseMap):
    """
    Create a chloropleth summary map of fluoride burden in Mexico
    """
    output_file = '../reports/figures/summary_fluoride.png'

    def requires(self):
        yield FluorideSummary()

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        # Import summary
        f_state_summary = pd.read_csv('processed/summary_fluoride.csv', index_col=0)
        # Import map of all states
        states = geopandas.read_file('interim/states')
        states2 = states[['NAME_1','geometry']] # Reduce columns
        states2.columns = ['state','geometry'] # Rename columns

        # Merge the map of the states with their summary statistics
        f_states = states2.merge(f_state_summary, on='state', how='left')
        f_states = f_states.fillna(0)

        fig = plt.figure(figsize=(40,40))
        self.draw_mexico(fig)
        ax=fig.axes[-1]

        divider = make_axes_locatable(ax)
        cax = divider.append_axes("right", size="3%", pad=0.1)

        f_states.plot(column='population', ax=ax, 
                    legend=True,  cax=cax, cmap='Blues',
                    edgecolor='black')
        ax.tick_params(
            axis='both', bottom=False, left=False,         
            labelbottom=False, labelleft=False) 

        for l in cax.yaxis.get_ticklabels():
            l.set_fontsize(36)

        cax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
        # ax.set_title('Population exposed to fluoride above 0.06 mg/(kg * day)', 
        #                     fontsize=60, pad=20)

        print(f'>> Making map of fluoride burden by state...')
        fig.savefig(self.output_file, bbox_inches='tight')