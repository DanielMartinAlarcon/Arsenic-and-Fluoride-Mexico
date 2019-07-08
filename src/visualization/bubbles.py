import geopandas
import luigi
from luigi.parameter import IntParameter, Parameter
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import pandas as pd
from sys import path

# Custom functions and other Luigi tasks
path.insert(0, '/app/src/')
path.insert(0, '/app/src/data_tasks')
path.insert(0, '/app/src/visualization')
from extract_maps import ProcessExternalFiles
from extract_samples import CleanAndExportSamples
from custom_functions import make_geodf
from maps_base import BaseMap


# --- Map drawing Luigi tasks --- #

class BubbleMap(BaseMap):
    """
    Class for maps of sites around Mexico, with area proportional to contamination
    """
    def requires(self):
        yield ProcessExternalFiles()
        yield CleanAndExportSamples()


class AllSitesMap(BubbleMap):
    """
    Read the cleaned list of contaminated sites, put them all on a map.
    """
    output_file = '../reports/figures/all_sites.png' 

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        samples, geo_samples = self.load_samples()
        fig = plt.figure(figsize=(40,40))
        self.draw_mexico(fig)
        geo_samples.plot(ax=fig.axes[-1], marker='o', markersize=30)
        # fig.axes[-1].set_title('CONAGUA Sampling Sites', fontsize=60, pad=20)
        print(f'>> Making map of all sites...')
        fig.savefig(self.output_file, bbox_inches='tight')


class AllContaminantsMap(BubbleMap):
    """
    Read the cleaned list of contaminated sites, make a map of Mexico 
    with all the contaminants together.
    """
    output_file = '../reports/figures/all_contaminants.png'

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        # Make map of all contaminants
        fig = plt.figure(figsize=(40,40))
        self.draw_mexico(fig)
        list_of_ions = []
        for ion in self.contaminant_list:
            list_of_ions.append(ion)
            self.draw_ion(fig, ion)
        self.make_legends(fig, list_of_ions)
        # fig.axes[-1].set_title('Water Contamination in Mexico', fontsize=60, pad=20)
        print(f'>> Making map of all contaminants...')
        fig.savefig(self.output_file, bbox_inches='tight')


class ArsenicAndFluorideMap(BubbleMap):
    """
    Read the cleaned list of contaminated sites, make a map of Mexico 
    with just arsenic and fluoride
    """
    output_file = '../reports/figures/arsenic_and_fluoride.png'

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        # Make map of arsenic and fluoride
        fig = plt.figure(figsize=(40,40))
        self.draw_mexico(fig)
        list_of_ions = []
        for ion in ['arsenic','fluoride']:
            list_of_ions.append(ion)
            self.draw_ion(fig, ion)
        self.make_legends(fig, list_of_ions)
        # fig.axes[-1].set_title('Arsenic and Fluoride Contamination in Mexico', fontsize=60, pad=20)
        print(f'>> Making map of arsenic and fluoride...')
        fig.savefig(self.output_file, bbox_inches='tight')

class EachContaminantMaps(BubbleMap):
    """
    Read the cleaned list of contaminated sites, make one map of Mexico for each
    the contaminants.
    """
    output_path = '../reports/figures/'

    def output(self):
        for ion in self.contaminant_list:
            yield luigi.LocalTarget(f'{self.output_path}{ion}.png')

    def run(self):
        for ion in self.contaminant_list:
            fig = plt.figure(figsize=(40,40))
            self.draw_mexico(fig)
            self.draw_ion(fig, ion)
            self.make_legends(fig)
            # fig.axes[-1].set_title(f'{ion.capitalize()} Contamination in Mexico',
            #                     fontsize=60, pad=20)
            print(f'>> Making {ion} map...')
            fig.savefig(f'{self.output_path}{ion}.png', bbox_inches='tight')


