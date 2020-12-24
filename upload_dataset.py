import os
from google.cloud import bigquery
from datetime import datetime 
import pandas as pd


def load_dataset_to_BQ(filename, table_id):

    df = pd.read_csv(filename, sep=',')
    
    client = bigquery.Client()

    job = client.load_table_from_dataframe(df, table_id)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(

        "Loaded {} rows and {} columns to {} at {}.".format(
            table.num_rows, len(table.schema), table_id, datetime.today()
        )
    )

    return table



'''
https://cloud.google.com/bigquery/docs/datasets
'''
def create_dataset(dataset_id):

    client = bigquery.Client()

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = "US"

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))



def delete_table(table_id):
    client = bigquery.Client()
    try:
        job = client.delete_table( table_id )
        job.result()
        print(f'{ table_id } was removed.')
    except:
        print(f'The { table_id } is not exist.')



def upload_dataset():
    load_dataset_to_BQ(f"{ os.getcwd() }/dataset/bill_of_materials.csv",  f"{ project_id }.bill_of_materials")
    load_dataset_to_BQ(f"{ os.getcwd() }/dataset/comp_boss.csv",  f"{ project_id }.comp_boss")
    load_dataset_to_BQ(f"{ os.getcwd() }/dataset/price_quote.csv",  f"{ project_id }.price_quote")

def delete_dataset():
    delete_table(f'{ project_id }.bill_of_materials')
    delete_table(f'{ project_id }.comp_boss')
    delete_table(f'{ project_id }.price_quote')

def main():
    create_dataset(project_id)
    delete_dataset()
    upload_dataset()

project_id = 'teste-dotz-292803.dotz'

if __name__ == '__main__':
    main()
    i = 0

df = pd.read_csv(f"{ os.getcwd() }/dataset/bill_of_materials.csv", sep=',')
import numpy as np
#print(df.replace({np.nan:None}).to_dict('records'))
print(df.to_string)




#print(pd.io.json.build_table_schema(df))

