import luigi

from sys import path
path.insert(0, '/app/src/data_tasks')
path.insert(0, '/app/src/visualization')

from extract_maps import ProcessExternalFiles
from extract_samples import CleanAndExportSamples
from bubbles import AllSitesMap
from bubbles import AllContaminantsMap
from bubbles import EachContaminantMaps
from bubbles import ArsenicAndFluorideMap
from sample_town_interactions import WhichTownsAreNearSites
from aggregate_nearby_sites import AggregateNearbySites
from make_summaries import ArsenicSummary
from make_summaries import FluorideSummary
from chloropleths import ArsenicMap
from chloropleths import FluorideMap

class FinalTask(luigi.Task):
    def requires(self):
        yield ProcessExternalFiles()
        yield CleanAndExportSamples()
        yield AllSitesMap()
        yield AllContaminantsMap()
        yield EachContaminantMaps()
        yield ArsenicAndFluorideMap()
        yield WhichTownsAreNearSites(radius=5)
        yield AggregateNearbySites(radius=5)
        yield ArsenicSummary()
        yield FluorideSummary()
        yield ArsenicMap()
        yield FluorideMap()

    def output(self):
        pass

    def run(self):
        pass

