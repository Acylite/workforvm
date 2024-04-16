
import subprocess
def install_requirements(requirements_file: str):
    try:
        # Execute pip install command
        subprocess.check_call(["pip", "install", "-r", requirements_file])
        print("Dependencies installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to install dependencies. {e}")
install_requirements('requirements.txt')
import apache_beam as beam
import io
import csv
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from typing import Tuple
import json 
from datetime import datetime

# Define the pipeline options
options = PipelineOptions()

def read_csv_file(readable_file):
    gcs_file = beam.io.filesystems.FileSystems.open(readable_file)
    reader = csv.reader(io.TextIOWrapper(gcs_file))
    next(reader)  # Skip the header line
    return reader

def process_data(row: list[str]):
    try:
        print(row)
        dt = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S UTC')
        date = dt.date()
        year = dt.year
        # Example transformation: convert text to uppercase
        transaction_amount = float(row[3])
        if year > 2009 and transaction_amount > 20:
            filtered_record: Tuple[str, float] = (str(date), float(transaction_amount))
            return filtered_record
        else:
            print('Got None')
            return None
            
    except Exception as e:
        print(f"Error processing row: {e}")
        return None

class WriteToJsonLines(beam.PTransform):
    def __init__(self, file_name):
        self._file_name = file_name

    def expand(self, pcoll):
        return (pcoll
                | 'format json' >> beam.Map(json.dumps)
                | 'write to text' >> beam.io.textio.WriteToText(self._file_name, shard_name_template='', file_name_suffix = '.jsonl.gz', compression_type = 'gzip'))
    
class ProcessDataTransform(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll
                | "Process Data" >> beam.Map(process_data)
                | "Filter None Values" >> beam.Filter(lambda x: x is not None))
    
def run():
    # Initialize the pipeline with DirectRunner
    with beam.Pipeline(options=options) as p:
        # Read data from GCS
        create = p | "Create GCS URI" >> beam.Create(['gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'])
        lines = create | "Read CSV Object" >> beam.FlatMap(lambda filename: read_csv_file(filename))
        processed_data = lines | "Process Data" >> ProcessDataTransform()
        combine_per_key =  processed_data | 'combine' >> beam.CombinePerKey(lambda values: sum(values))
        json_objects = combine_per_key | "Convert to JSON" >> beam.Map(lambda row: {
            "date": row[0],
            "total_amount": row[1],
            # Map other columns as needed
        })
        json_objects | 'Write to JSON' >> WriteToJsonLines('output/output_file')
def test_process_data_transform():
    # Define test data
    test_data = [["2024-04-16 10:00:00 UTC", "123", "456", "30"],  # Valid data
                 ["2022-01-01 10:00:00 UTC", "123", "456", "10"],  # Year <= 2009
                 ["2024-04-16 10:00:00 UTC", "123", "456", "10"],  # Amount <= 20
                 ["invalid_data", "123", "456", "30"]]  # Invalid date format
    
    # Expected output for the valid data
    expected_output = [("2024-04-16", 30.0)]
    
    # Run the test pipeline
    with TestPipeline() as p:
        test_result = (p | beam.Create(test_data) | ProcessDataTransform())
        assert_that(test_result, equal_to(expected_output))
    print("test pipeline ran successfully.")

if __name__ == "__main__":
    test_process_data_transform() #This test throws an error on the final step because it is not a date format 
    run()
