import geopandas
import luigi
import numpy as np
import pandas as pd
from sys import path

# Custom functions and other Luigi tasks
path.insert(0, '/app/src')
from custom_functions import make_geodf
from aggregate_nearby_sites import AggregateNearbySites
from sample_town_interactions import WhichTownsAreNearSites


class ArsenicSummary(luigi.Task):
    """
    Create and export a summary of the arsenic burden in each state in Mexico 
    """
    output_file = 'processed/summary_arsenic.csv'

    def requires(self):
        yield AggregateNearbySites()
        yield WhichTownsAreNearSites()

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        # Import dataset as geopandas dataframe
        towns3 = make_geodf(pd.read_csv('processed/towns_with_exposure.csv', index_col=0))

        # Consider towns with nonzero exposure only        
        as_towns = towns3[towns3['arsenic'] > 0].copy()

        # Calculate daily dosage, multiplying by 0.001 to convert to mg/L
        as_towns['dosage'] = as_towns['arsenic'] * 0.001 * (0.71*2/70 + 0.29*1/25)

        # Factor that determines the relationship between As exposure and cancer risk
        cancer_slope_factor = 1.5

        # Calculate individual cancer risk
        as_towns['individual_cancer_risk'] = as_towns['dosage'] * cancer_slope_factor

        # Calculate cancer incidence
        as_towns['cancer_incidence'] = as_towns['individual_cancer_risk'] * as_towns['population']

        # Calculate the population exposed to As above the limit of 10 µg/L
        # and their expected cancer incidence
        as_state_summary = as_towns[as_towns.arsenic >= 10]\
                        .groupby('state').sum()[['population', 'cancer_incidence']]
        as_state_summary = as_state_summary.reset_index()
        as_state_summary.state = as_state_summary.state.replace({
                    'Coahuila de Zaragoza':'Coahuila',
                    'Michoacán de Ocampo':'Michoacán',
                    'Veracruz de Ignacio de la Llave':'Veracruz'})
        as_state_summary = as_state_summary\
                .sort_values('cancer_incidence', ascending=False)\
                .reset_index(drop=True)

        as_state_summary.to_csv(self.output_file)


class FluorideSummary(luigi.Task):
    """
    Create and export a summary of the fluoride burden in each state in Mexico 
    """
    output_file = 'processed/summary_fluoride.csv'

    def requires(self):
        yield AggregateNearbySites()
        yield WhichTownsAreNearSites()

    def output(self):
        return luigi.LocalTarget(self.output_file)

    def run(self):
        # Import dataset as geopandas dataframe
        towns3 = make_geodf(pd.read_csv('processed/towns_with_exposure.csv', index_col=0))

        # Consider towns with nonzero exposure only        
        f_towns = towns3[towns3['fluoride'] > 0].copy()

        # Calculate daily dosage, multiplying by 0.001 to convert to mg/L
        f_towns['dosage'] = f_towns['fluoride'] * 0.001 * (0.71*2/70 + 0.29*1/25)

        # Calculate the population exposed to F above the reference dose of 0.06 mg/(kg * day)
        f_state_summary = f_towns[f_towns['dosage'] >= 0.06]\
                        .groupby('state').sum()['population']
        f_state_summary = f_state_summary.reset_index()
        f_state_summary.state = f_state_summary.state.replace({
                'Coahuila de Zaragoza':'Coahuila', 
                'Michoacán de Ocampo':'Michoacán',
                'Veracruz de Ignacio de la Llave':'Veracruz'})
        f_state_summary = f_state_summary\
                .sort_values('population', ascending=False)\
                .reset_index(drop=True)
        
        f_state_summary.to_csv(self.output_file)