#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A workflow that writes to a BigQuery table with nested and repeated fields.

Demonstrates how to build a bigquery.TableSchema object with nested and repeated
fields. Also, shows how to generate data to be written to a BigQuery table with
nested and repeated fields.
"""

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

PROJECT='my_project_here'
BUCKET='my_bucket_here'
SCHEMA='my_schema_here'
TABLE='my_table_here'
OUTPUT='{0}:{1}.{2}'.format(PROJECT,SCHEMA,TABLE)
INPUT_TOPIC='projects/{0}/topics/bigquery'.format(PROJECT)
DATASET='{0}:{1}'.format(PROJECT,SCHEMA)

def split_fn(lines):
  import re
  return re.split(' ', lines)

def run():
  argv = [
     '--project={0}'.format(PROJECT),
     '--job_name=examplejob2',
     '--save_main_session',
     '--staging_location=gs://{0}/staging/'.format(BUCKET),
     '--temp_location=gs://{0}/staging/'.format(BUCKET),
     '--runner=DataflowRunner',
     '--output={0}:{1}.{2}'.format(PROJECT,SCHEMA,TABLE)
  ]
  
  options = PipelineOptions(argv)
  options.view_as(StandardOptions).streaming = True
  
  # with beam.Pipeline(argv=argv) as p:
  with beam.Pipeline(options=options) as p:

    
    import os
    from google.cloud import bigquery
    bq = bigquery.Client()
    
    query = "SELECT * FROM {} LIMIT 0".format(OUTPUT)
    resp = bq.run_sync_query(query)
    # resp = beam.io.Read(beam.io.BigQuerySource(query=query)) 
    table_schema = resp.schema
    
    dataset = bq.dataset(DATASET)
    table = dataset.table(TABLE)
    
    from apache_beam.io.gcp.internal.clients import bigquery  # pylint: disable=wrong-import-order, wrong-import-position
    

    """    
    # Commented because we get the schema from the table itself
    table_schema = bigquery.TableSchema()

    # Fields that use standard types.
    kind_schema = bigquery.TableFieldSchema()
    kind_schema.name = 'kind'
    kind_schema.type = 'string'
    kind_schema.mode = 'nullable'
    table_schema.fields.append(kind_schema)

    full_name_schema = bigquery.TableFieldSchema()
    full_name_schema.name = 'fullName'
    full_name_schema.type = 'string'
    full_name_schema.mode = 'required'
    table_schema.fields.append(full_name_schema)

    age_schema = bigquery.TableFieldSchema()
    age_schema.name = 'age'
    age_schema.type = 'integer'
    age_schema.mode = 'nullable'
    table_schema.fields.append(age_schema)

    gender_schema = bigquery.TableFieldSchema()
    gender_schema.name = 'gender'
    gender_schema.type = 'string'
    gender_schema.mode = 'nullable'
    table_schema.fields.append(gender_schema)

    # A nested field
    phone_number_schema = bigquery.TableFieldSchema()
    phone_number_schema.name = 'phoneNumber'
    phone_number_schema.type = 'record'
    phone_number_schema.mode = 'nullable'

    area_code = bigquery.TableFieldSchema()
    area_code.name = 'areaCode'
    area_code.type = 'integer'
    area_code.mode = 'nullable'
    phone_number_schema.fields.append(area_code)

    number = bigquery.TableFieldSchema()
    number.name = 'number'
    number.type = 'integer'
    number.mode = 'nullable'
    phone_number_schema.fields.append(number)
    table_schema.fields.append(phone_number_schema)

    # A repeated field.
    children_schema = bigquery.TableFieldSchema()
    children_schema.name = 'children'
    children_schema.type = 'string'
    children_schema.mode = 'repeated'
    table_schema.fields.append(children_schema)
    """

    def create_random_record(record_id):
      return {'kind': 'kind' + record_id, 'fullName': 'fullName'+record_id,
              'age': int(record_id) * 10, 'gender': 'male',
              'phoneNumber': {
                  'areaCode': int(record_id) * 100,
                  'number': int(record_id) * 100000},
              'children': ['child' + record_id + '1',
                           'child' + record_id + '2',
                           'child' + record_id + '3']
             }

    # pylint: disable=expression-not-assigned
    
    lines = p | 'Read PubSub' >> beam.io.ReadStringsFromPubSub(INPUT_TOPIC) | beam.WindowInto(window.FixedWindows(15))
    record_ids = lines | 'Split' >> (beam.FlatMap(split_fn).with_output_types(unicode))
    #record_ids = p | 'CreateIDs' >> beam.Create(['1', '2', '3', '4', '5'])
    records = record_ids | 'CreateRecords' >> beam.Map(create_random_record)
    records | 'Write' >> beam.io.Write(
        beam.io.BigQuerySink(
            # known_args.output,
            OUTPUT,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))

    # Run the pipeline (all operations are deferred until run() is called).


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
