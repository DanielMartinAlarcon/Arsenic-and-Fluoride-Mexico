import geopandas
import luigi
import numpy as np
import pandas as pd
from sys import path
from luigi.parameter import IntParameter

# Custom functions and other Luigi tasks
path.insert(0, '/app/src/')
from extract_maps import ProcessExternalFiles
from extract_samples import CleanAndExportSamples
from custom_functions import make_geodf


class WhichTownsAreNearSites(luigi.Task):
    """
    Reduce the full list of towns to just those that are near a sampling site.
    """
    output_file = 'processed/towns_near_sites.csv'
    # Towns within this radius of a site are considered to have the contaminant
    # levels of that site. Units are kilometers.
    radius = IntParameter(default=5)

    def requires(self):
        yield ProcessExternalFiles()
        yield CleanAndExportSamples()

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        # --- Process samples --- #
        print('>> Importing list of samples...')
        samples = pd.read_csv('processed/samples.csv', index_col=0)
        # Make a geopandas dataframe from it
        geo_samples = make_geodf(samples)

        # --- Process towns --- #
        print('>> Importing list of towns...')
        towns = geopandas.read_file('interim/towns')
        # Remove most columns, rename the rest
        towns = towns[['NOM_ENT', 'NOM_MUN', 'NOM_LOC', 'POBTOT', 'lon_dd',
            'lat_dd', 'geometry']].copy()
        towns = towns.rename(columns={
            'NOM_ENT':'state',
            'NOM_MUN':'municipality',
            'NOM_LOC':'name',
            'POBTOT':'population',
            'lon_dd':'longitude',
            'lat_dd':'latitude'})

        def are_there_nearby_sites(row):
            """
            Apply this function to the rows in 'towns'; it will figure out whether
            there are sites located within [radius] kilometers of that town.
            This reduces the search space for heavier operations later.
            """
            lat = row.latitude
            lon = row.longitude
            nearby_sites = samples[np.sqrt((samples.longitude - lon)**2 + \
                                (samples.latitude - lat)**2)*102.47 < self.radius]

            return len(nearby_sites) > 0

        # Add a column that says whether there are nearby sites
        print('>> Calculating which towns are near sites...')
        towns['sites_5k'] = towns.apply(are_there_nearby_sites, axis=1)

        # Export this shorter list to file
        towns2 = towns[towns['sites_5k'] == True]
        towns2 = towns2.drop(columns='sites_5k')
        towns2 = towns2.reset_index(drop=True)
        towns2.to_csv(self.output_file)
