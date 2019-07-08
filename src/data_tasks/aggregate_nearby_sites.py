import luigi
import numpy as np
import pandas as pd
from sys import path
from luigi.parameter import IntParameter

# Custom functions and other Luigi tasks
path.insert(0, '/app/src/')
from custom_functions import make_geodf
from extract_maps import ProcessExternalFiles
from extract_samples import CleanAndExportSamples
from sample_town_interactions import WhichTownsAreNearSites


class AggregateNearbySites(luigi.Task):
    """
    For each town that has nearby sampling sites, average the contaminant
    levels for all nearby sites.
    """
    output_file = 'processed/towns_with_exposure.csv'

    # Towns within this radius of a site are considered to have the contaminant
    # levels of that site. Units are kilometers.
    radius = IntParameter(default=5)

    def requires(self):
        yield ProcessExternalFiles()
        yield CleanAndExportSamples()
        yield WhichTownsAreNearSites(radius=5)

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        # --- Process towns --- #
        print('>> Importing list of towns near sites...')
        towns2 = pd.read_csv('interim/towns_near_sites.csv', index_col=0)
        towns2 = make_geodf(towns2)

        # --- Process samples --- #
        print('>> Importing list of samples...')
        samples = pd.read_csv('processed/samples.csv', index_col=0)
        # Make a geopandas dataframe from it
        geo_samples = make_geodf(samples)
        
        sites_5km = []
        # This step is long
        print('>> Calculating summary statistics for sites near each town...')
        for idx, row in towns2.iterrows():

            # Filter for nearby sites, put them in a dataframe
            lat = row.latitude
            lon = row.longitude
            nearby_sites = samples[np.sqrt((samples.longitude - lon)**2 + (samples.latitude - lat)**2)*102.47 < self.radius]
            
            ## Create a Series with summary stats from that dataframe
            
            # Average contamination levels
            town_summary = nearby_sites.mean()[['arsenic', 'cadmium', 'chromium', 'mercury',
                            'lead', 'fluoride']]
            
            # How many sites are within 5km
            town_summary['number_nearby_sites'] = len(nearby_sites)
            
            sites_5km.append(town_summary)
            
            if idx % 1000 == 0:
                print(f'Processing town {idx} / {len(towns2)}')
            
        towns2_pollution_columns = pd.concat(sites_5km, axis=1).transpose()
        towns3 = pd.concat([towns2, towns2_pollution_columns], sort=False, axis=1)
        towns3.to_csv(self.output_file)
