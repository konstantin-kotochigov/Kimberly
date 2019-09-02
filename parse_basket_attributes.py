import pandas
import numpy as np

data_path = "/user/kkotochigov/kimberly_basket_sample"
x = spark.read.format("com.databricks.spark.avro").load(data_path)

# Extract FPC, and the list of (id, value) pairs
def process_record(r):
  result_id = ""
  for id_element in r['id']:
    if 'member6' in id_element:
      if ((id_element['member6']['id']['primary'] == 10005) and (id_element['member6']['id']['secondary'] == -1)):
        result_id = str(id_element['member6']['value'])
  result_attrs = []
  for attr in r['attributes']:
    attr_type = [field for field in attr.__fields__ if attr[field] != None]
    if len(attr_type)>1:
      raise Exception("multiple attribute types")
    attr_type=attr_type[0]
    id_attr = attr[attr_type]['id']
    id_attr_primary = str(id_attr['primary'])
    id_attr_secondary = str(id_attr['secondary'])
    if attr_type != "member0":
      attr_value = str(attr[attr_type]['value'])
    else:
      attr_value = "1"
    result_attrs.append(("_".join([id_attr_primary, id_attr_secondary]), attr_value))
  return (result_id, result_attrs)

sparse_df = x.rdd.map(lambda x:process_record(x)).collect()
sparse_df_dict = [dict(x[1]) for x in sparse_df]
df = pandas.DataFrame(sparse_df_dict)
df['id'] = [x[0] for x in sparse_df]
df = df.fillna(0)
df.rename(columns={"id":"tpc"}, inplace=True)
