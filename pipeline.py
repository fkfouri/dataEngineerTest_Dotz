import re
import csv
import io
import pandas as pd

import apache_beam as beam

inputs_pattern = 'dataset/comp_boss.csv'
outputs_prefix = 'outputs/part'

boss= None
price = None

with beam.Pipeline() as pipeline:
    boss = (
        pipeline
        | 'Read File' >> beam.io.ReadFromText(inputs_pattern)
        #| 'Write' >> beam.io.WriteToText(outputs_prefix)

        )

with beam.Pipeline() as pipeline:
    price = (
        pipeline
        | 'Read File' >> beam.io.ReadFromText('dataset/price_quote.csv')
        )


print(boss, price)

def create_dataframe(readable_file):
    # Open a channel to read the file from GCS
    gcs_file = beam.io.filesystems.FileSystems.open(readable_file)

    # Read it as csv, you can also use csv.reader
    csv_dict = csv.DictReader(io.TextIOWrapper(gcs_file))

    # Create the DataFrame
    dataFrame = pd.DataFrame(csv_dict)
    print(dataFrame.to_string())


p = beam.Pipeline()
(p | beam.Create(['dataset/comp_boss.csv'])
   | beam.FlatMap(create_dataframe)
)

#p.run()