from google.cloud import bigquery

client = bigquery.Client()

# Perform a query.
QUERY = ( 
    ' SELECT tube_assembly_id FROM `dotz.bill_of_materials` '
    ' order by tube_assembly_id desc '
    ' LIMIT 100')

QUERY = ('''
    select distinct tube_assembly_id from dotz.bill_of_materials
    order by tube_assembly_id desc
    LIMIT 10
''')

query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.tube_assembly_id)
