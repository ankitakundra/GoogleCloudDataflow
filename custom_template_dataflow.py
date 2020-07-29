import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class DataflowExample(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--filter_val', type=str)

class FilteringDoFn(beam.DoFn):
    def __init__(self, filter_val):
        self.filter_val = filter_val
    def process(self, element):
        if element['gender'] == self.filter_val.get():
            yield element
        else:
            return  # Return nothing

logging.getLogger().setLevel(logging.INFO)

pipeline_options = PipelineOptions()
# Create pipeline.
with beam.Pipeline(options=pipeline_options) as p:
    def print_row(element):
        logging.info("the count is ", element)
    my_options = pipeline_options.view_as(DataflowExample)
    select_query = (p | 'QueryTableStdSQL' >> beam.io.Read(beam.io.BigQuerySource(
        query='SELECT gender FROM ' \
              '`startgcp-268623.lake.usa_names`',
        use_standard_sql=True)))
    select_query | beam.ParDo(FilteringDoFn(my_options.filter_val)) | beam.combiners.Count.Globally() \
    | 'Print result' >> beam.Map(print_row)
    p.run().wait_until_finish()
