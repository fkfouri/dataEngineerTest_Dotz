import apache_beam as beam


x = [3, 8, 12] | beam.Map(lambda x : 3*x)

print(x)

x = [('Jan',3), ('Jan',8), ('Feb',12)] | beam.GroupByKey()

print(x)

