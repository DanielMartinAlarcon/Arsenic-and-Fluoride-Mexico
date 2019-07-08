import geopandas
import luigi
from luigi.parameter import IntParameter, Parameter
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import pandas as pd
from sys import path

# Custom functions and other Luigi tasks
from custom_functions import make_geodf


# --- Map drawing Luigi tasks --- #

class BaseMap(luigi.Task):
    """
    Base class for mapmaking tasks.
    """

    # --- Global variables --- #

    color_dict = {'arsenic':'red',
                'cadmium':'darkcyan',
                'chromium':'magenta',
                'mercury':'brown',
                'lead':'green',
                'fluoride':'blue'}

    limit_dict = {'arsenic':10,
                'cadmium':5,
                'chromium':100,
                'mercury':2,
                'lead':15,
                'fluoride':1500}

    # Arbitrary number that determines the area of the circles on the map.
    # This number gets multiplied by the area of the circles, which is 
    # proportional to the ratio of contaminant level to contaminant limit.
    circle_multiplier = 100

    contaminant_list = ['arsenic', 'cadmium','chromium','mercury','lead','fluoride']

        
    def load_helper_maps(self):
        # Import helper maps
        world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        neighbors = world[world.name.isin(['United States of America','Guatemala','Belize','Honduras','El Salvador'])]
        mexico = geopandas.read_file('interim/country')
        # mexico = world[world.name == 'Mexico'] # Uncomment to use lower-resolution map of Mexico.
        states = geopandas.read_file('interim/states')
        
        return world, neighbors, mexico, states

    def load_samples(self):
        # Import dataset
        samples = pd.read_csv('processed/samples.csv', index_col=0)
        # Make a geopandas dataframe from it
        geo_samples = make_geodf(samples)

        return samples, geo_samples

    def draw_mexico(self, fig):
        """
        Draw an empty map of Mexico (high-res) and neighboring countries, 
        with a blue ocean.
        """
        _, neighbors, _, states = self.load_helper_maps()

        ax = fig.add_subplot(1,1,1)
        ax.set_facecolor('#c9e4ff')
        ax.tick_params(
            axis='both', bottom=False, left=False,         
            labelbottom=False, labelleft=False) 
        ax.set_xlim(-118, -86)
        ax.set_ylim(14,33)
        
        neighbors.plot(ax=ax, color='white', edgecolor='black')
        states.plot(ax=ax, color='white', edgecolor='black')  
        return fig

    def draw_ion(self, fig, ion):
        """
        Add a map of contaminated sites to an existing map (the topmost axes in fig).
        """
        _, geo_samples = self.load_samples()
        contaminated_sites = geo_samples[geo_samples[ion] >= self.limit_dict[ion]]
        markersize = contaminated_sites[ion] / self.limit_dict[ion] * self.circle_multiplier
        contaminated_sites.plot(ax=fig.axes[-1], markersize=markersize, 
                                color=self.color_dict[ion], alpha=0.5)
        return fig

    def make_legends(self, fig, list_of_ions=[]):
        """
        Add legends to an existing map (the topmost axes in fig).
        Only add contaminant patches if there's more than one contaminant.
        """
        ax=fig.axes[-1]

        # Make a legend to show the meaning of circle sizes
        for area in [1, 10, 100]:
            ax.scatter([0], [0], c='black' , alpha=0.9, s=area*self.circle_multiplier,
                        label=str(area) + ' x')
        legend1 = ax.legend(scatterpoints=1, frameon=True,
                labelspacing=1, loc='lower left', fontsize=40, bbox_to_anchor=(0.03,0.05),
                    title="Concentration\n(Multiples of\nlimit value)", title_fontsize=40)
        
        fig.gca().add_artist(legend1)
            
        if len(list_of_ions) > 1:
            # Make a legend for the substances in the figure and their limits
            patch_list =[]
            for ion in list_of_ions:
                label = ion.capitalize()
                color = self.color_dict[ion]
                patch_list.append(patches.Patch(facecolor=color, label=label, alpha=0.9, 
                                                linewidth=2, edgecolor='black'))

            ax.legend(handles=patch_list, fontsize=40, loc='lower left',
                    bbox_to_anchor = (.2,0.05), title_fontsize=45)
