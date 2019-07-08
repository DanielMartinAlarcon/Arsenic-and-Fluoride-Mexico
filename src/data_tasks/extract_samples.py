import luigi
from luigi.parameter import IntParameter, Parameter
import pandas as pd
import numpy as np

class CleanAndExportSamples(luigi.Task):
    """
    Read the original Excel file, extract sample data and turn it into
    a CSV of clean data with column names in English.
    """
    source_file = 'raw/CONAGUA_2017.xlsx'
    output_file = "samples.csv"

    def output(self):
        return luigi.LocalTarget(f"processed/{self.output_file}")

    def run(self):
        #--- Declare global variables ---#
        contaminant_limits = {'arsenic':10,'cadmium':5,'chromium':100,'mercury':2,
                'lead':15,'fluoride':1500}
        contaminant_list = ['arsenic', 'cadmium','chromium',
                            'mercury','lead','fluoride']

        #--- Read data from the Excel file ---#

        # Create an ordered dictionary of pandas DataFrames
        all_data = pd.read_excel(self.source_file, sheet_name=[0,1,2])

        # Sheet 0 contains information about all the sites that were sampled
        sites = all_data[0].copy()

        # Sheet 1 contains many samples collected at each site.
        samples = all_data[1][['CLAVE SITIO','CLAVE MONITOREO',
                            'AS_TOT','CD_TOT','CR_TOT',
                            'HG_TOT','PB_TOT',
                            'FLUORUROS_TOT']].copy()
        
        #--- Process sites ---#

        # For some reason, the longitude value for one site is double what it
        # should be, according to cross-referencing with Google Maps.
        # This line fixes the problem.
        sites.LONGITUD = sites.LONGITUD.replace({-233.2436:-116.6218})

        #--- Process samples ---#

        # Change column names to English
        col_dict = {'CLAVE SITIO':'site', 
                    'CLAVE MONITOREO':'measurement',
                    'AS_TOT':'arsenic',
                    'CD_TOT':'cadmium', 
                    'CR_TOT':'chromium',
                    'HG_TOT':'mercury',
                    'PB_TOT':'lead',
                    'FLUORUROS_TOT':'fluoride'}

        samples = samples.rename(columns=col_dict)

        # Remove any rows where all contaminant measurements are NAN
        samples = samples.dropna(how='all', subset=contaminant_list)

        # The numerical columns all include samples under the detection limits, 
        # encoded as strings beginning with "<". Replace them with zeroes.
        for col in contaminant_list:
            samples[col] = samples[col].apply(
                lambda x: 0 if type(x)==str else x)
            
        # The sample data came in mg/L. Convert to µg/L
        for col in contaminant_list:
            samples[col] = samples[col].apply(lambda x: 1000*x)
            
        # The data spans multiple sampling events for each site.
        # Aggregate all of those into an average value for each site.
        # Note that this operation ignores NANs when calculating mean.
        samples = samples.groupby('site', as_index=False).agg('mean')


        #--- Tag samples with the coordinates and type of their site ---#

        # Dictionary of all sites with their latitude
        lat_dict = {}
        for row in sites.iterrows():
            key = row[1]['CLAVE SITIO']
            val = row[1]['LATITUD']
            lat_dict[key] = val
            
            
        # Dictionary of all sites with their longitude
        long_dict = {}
        for row in sites.iterrows():
            key = row[1]['CLAVE SITIO']
            val = row[1]['LONGITUD']
            long_dict[key] = val
            
        # Dictionary of English names for each original site type
        site_type_english = {
            'PRESA':'Dam', 'POZO':'Well', 'RÍO':'River', 'DESCARGA':'Discharge',
            'CANAL':'Canal','SISTEMA DE RIEGO DE LA PRESA':'Dam', 
            'OCÉANO-MAR':'Ocean', 'BAHIA':'Ocean', 'LAGUNA':'Lagoon',
            'ARROYO':'Stream', 'DESCARGA INDUSTRIAL':'Industrial Discharge',
            'ESTERO':'Swamp', 'MANGLAR':'Mangrove', 'ESTUARIO':'Estuary',
            'LAGO':'Lake', 'CENOTE':'Cenote', 'MANANTIAL':'Spring',
            'DESCARGA MUNICIPAL':'Municipal Discharge', 'RIO':'River',
            'EMBALSE ARTIFICIAL':'Temporary Dam', 'MAR':'Ocean', 
            'NORIA':'Water Wheel', 'MARISMA':'Swamp',
            'TRANSICIÓN RÍO-MAR':'River-Ocean Transition',
            'DREN':'Drainage', 'OCEANO-MAR':'Ocean', 'PrEsa':'Dam',
            'Canal':'Canal','Arroyo':'Stream', 'Pozo':'Well', 'Lago':'Lake',
            'CIÉNEGA':'Swamp'}

        # Dictionary of all sites with their site type
        site_type_dict = {}
        for row in sites.iterrows():
            key = row[1]['CLAVE SITIO']
            val = site_type_english[row[1]['SUBTIPO CUERPO AGUA']]
            site_type_dict[key] = val
            
        # For each sample, add coordinates and site type
        samples['latitude'] = samples['site'].map(lat_dict)
        samples['longitude'] = samples['site'].map(long_dict)
        samples['type'] = samples['site'].map(site_type_dict)

        # Reorder the columns to be more convenient
        reordered_column_titles = ['site','type','latitude','longitude'] \
                                    + contaminant_list

        samples = samples.reindex(columns = reordered_column_titles)

        #--- Export ---#
        samples.to_csv(self.output().path)