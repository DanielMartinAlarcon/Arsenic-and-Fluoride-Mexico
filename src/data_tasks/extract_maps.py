import luigi
import os
from luigi.contrib.external_program import ExternalProgramTask
from luigi.parameter import IntParameter, Parameter

class UnzipToInterim(ExternalProgramTask):
    """
    Extract a single file from the external/ to interim/
    Expects a file name without the '.zip', and assumes it's 
    in external/
    """
    file_name = Parameter()

    def output(self):
        return luigi.LocalTarget(f"interim/{self.file_name}")

    def program_args(self):
        return ["unzip", "-u", "-q",
                "-d", self.output().path,
                f'external/{self.file_name}.zip']


class ProcessExternalFiles(luigi.WrapperTask):
    """
    Wrapper task that executes UnzipToInterim for each zipped folder in external,
    and reports as complete once they've all been accounted for.
    """
    # List of zipped files in external/, each stripped of '.zip'
    file_names = [f[:-4] for _, _, files in os.walk("external/") for f in files if f.endswith('.zip')]

    def requires(self):
        for name in self.file_names:
            yield UnzipToInterim(name)
