import logging
from typing import Callable, Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import vastdb
import pyarrow as pa
import apache_beam.typehints.schemas as schemas

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run(
    input_text: str,
    beam_options: Optional[PipelineOptions] = None,
    vastdb_endpoint: str = "http://localhost:8080",
    vastdb_access_key_id: str = "7XB5EJWVTRPELZHRE0D0",
    vastdb_secret_access_key: str = "fBnyOdfw3bAjmUzcp2smbDbOm950qc4KxG/jJg1i",
    vastdb_bucket_name: str = "vastdb",
    vastdb_schema_name: str = "airbyte_demo",
    vastdb_table_name: str = "beam4",
    test: Callable[[beam.PCollection], None] = lambda _: None,
) -> None:
    with beam.Pipeline(options=beam_options) as pipeline:

        window_size = 100
        vast_table_config = {
            'endpoint': vastdb_endpoint,
            'access_key_id': vastdb_access_key_id,
            'secret_access_key': vastdb_secret_access_key,
            'bucket_name': vastdb_bucket_name,
            'schema_name': vastdb_schema_name,
            'table_name': vastdb_table_name,
            'pa_schema': pa.schema([
                ('id', pa.int64()),
                ('first_name', pa.utf8()),
                ('last_name', pa.utf8())
                ])
        }

        elements = (
            pipeline
            | "Create elements" >> beam.Create([
                { 'id': 1, 'first_name': 'John', 'last_name': 'Doe' },
                { 'id': 2, 'first_name': 'Jane', 'last_name': 'Doe' }
                ])
            | "Make Batches" >> beam.WindowInto(
                    beam.window.FixedWindows(window_size), 
                    trigger=beam.trigger.AfterWatermark(), 
                    accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING
                    )
            # | "Log Before Sink" >> beam.Map(lambda x: (logger.info(f'Element before sink: {x}') or x))
            | "Write To VastDB" >> VastDBSink(**vast_table_config)
        )

        # Used for testing only.
        test(elements)


class VastDBSink(beam.PTransform):
    def __init__(self, endpoint, access_key_id, secret_access_key, bucket_name, schema_name, table_name, pa_schema):
        super().__init__()
        self.endpoint = endpoint
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.bucket_name = bucket_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.pa_schema = pa_schema
        self.logger = logging.getLogger(__name__)

    def expand(self, pcoll):
        def process_batch(batch):
            if batch is None:
                self.logger.error('Batch is None')
                raise ValueError('Batch is None')
        
            session = vastdb.connect(endpoint=self.endpoint, access=self.access_key_id, secret=self.secret_access_key)
            with session.transaction() as tx:
                bucket = tx.bucket(self.bucket_name)

                schema = bucket.schema(self.schema_name, fail_if_missing=False)
                if schema is not None:
                    self.logger.debug(f'Found existing schema: {self.schema_name}')
                else:
                    schema = bucket.create_schema(self.schema_name)
                    self.logger.debug(f'Created schema: {self.schema_name}')

                table = schema.table(name=self.table_name, fail_if_missing=False)
                if table is not None:
                    # table.drop()
                    self.logger.debug(f'Found existing table: {table} with columns: {table.columns()}')
                else:
                    table = schema.create_table(table_name=self.table_name, columns=self.pa_schema)
                    self.logger.debug(f'Created table: {table} with columns: {table.columns()}')
  
                pa_table = self.create_pyarrow_table(batch, self.pa_schema)
                try:
                    table.insert(pa_table)
                except Exception as e:
                    self.logger.error(f'Error inserting data: {e}')

        return pcoll | beam.Map(process_batch)

    def create_pyarrow_table(self, beam_batch, pa_schema: pa.schema) -> pa.Table:
        self.logger.debug(f'Creating PyArrow table from Beam batch {beam_batch}')
        
        if isinstance(beam_batch, set):
            colnames = pa_schema.names
            beam_list = list(beam_batch)
            beam_batch = {colnames[i]: value for i, value in enumerate(beam_list)}
        elif isinstance(beam_batch, dict):
            # no need to convert
            pass
        else:
            raise ValueError(f'Unsupported Beam batch type {type(beam_batch)}')
        
        pa_table = pa.Table.from_struct_array(pa.array([beam_batch]))
        self.logger.debug(f'Created PyArrow table: {pa_table}')
        return pa_table

    