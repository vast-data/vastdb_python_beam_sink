# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from apache_beam.options.pipeline_options import PipelineOptions

from my_app import app


if __name__ == "__main__":
    import argparse
    import logging

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--vastdb-endpoint",)
    parser.add_argument("--vastdb-access-key-id")
    parser.add_argument("--vastdb-secret-access-key")
    parser.add_argument("--vastdb-bucket-name")
    parser.add_argument("--vastdb-schema-name")
    parser.add_argument("--vastdb-table-name")

    args, beam_args = parser.parse_known_args()

    beam_options = PipelineOptions(
        save_main_session=True,
        setup_file="./setup.py",
        direct_num_workers=1,  # Set the number of workers to 1 for debugging
        direct_running_mode='in_memory'  # Use in-memory mode for debugging
    )
    app.run(
        vastdb_endpoint=args.vastdb_endpoint,
        vastdb_access_key_id=args.vastdb_access_key_id,
        vastdb_secret_access_key=args.vastdb_secret_access_key,
        vastdb_bucket_name=args.vastdb_bucket_name,
        vastdb_schema_name=args.vastdb_schema_name,
        vastdb_table_name=args.vastdb_table_name,
        beam_options=beam_options,
    )
