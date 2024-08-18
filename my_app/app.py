import logging
from typing import Callable, Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import vastdb
import pyarrow as pa
import apache_beam.typehints.schemas as schemas
from .vastdbsink import VastDBSink

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run(
    vastdb_endpoint: str,
    vastdb_access_key_id: str,
    vastdb_secret_access_key: str,
    vastdb_bucket_name: str,
    vastdb_schema_name: str,
    vastdb_table_name: str,
    beam_options: Optional[PipelineOptions] = None,
    test: Callable[[beam.PCollection], None] = lambda _: None,
) -> None:
    
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
    
    with beam.Pipeline(options=beam_options) as pipeline:
        
        batch_size = 3
        elements = (
            pipeline
            | "Create elements" >> beam.Create([
                (1, {'first_name': 'John', 'last_name': 'Doe' }),
                (2, {'first_name': 'Jane', 'last_name': 'Doe' }),
                ])
            | "Group into batches" >> beam.GroupIntoBatches(batch_size)
            | "Write To VastDB" >> VastDBSink(batch_size, **vast_table_config)
        )

        # Used for testing only.
        test(elements)


