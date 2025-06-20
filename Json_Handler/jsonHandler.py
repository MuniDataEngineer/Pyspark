from pyspark.sql.functions import col, explode_outer,to_json
from pyspark.sql.types import StructType,ArrayType
from fieldNameValidation import fieldNameValidation

def jsonHandler(df):
  #collect the complex datatype fields (array, struct)
  complex_fields = [(field.name, field.dataType) for field in df.schema.fields if isinstance(field.dataType,(ArrayType,StructType))]

#exlpload and flatten
  while complex_fields:
    for field_name, field_dataType in complex_fields:
      if isinstance(field_dataType, ArrayType):
        df = df.withColumn(f"{field_name}_raw", to_json(col(field_name)))
        df = df.withColumn(field_name,explode_outer(col(field_name))) #exploading Array type fields
      elif isinstance(field_dataType, StructType):
        expanded_fields = [col(f"{field_name}.{fields}").alias(f"{field_name}_{fields}") for fields in field_dataType.names] # collect and rename the child names 
        df = df.withColumn(f"{field_name}_raw", to_json(col(field_name)))
        df = df.select("*",*expanded_fields).drop(field_name)
    #reset 
    complex_fields = [(field.name, field.dataType) for field in df.schema.fields if isinstance(field.dataType,(ArrayType,StructType))]
     
  #Field name validation
  raw_fields =  [col for col in df.columns]
  new_fields = fieldNameValidation(raw_fields)
  df = df.toDF(*new_fields)
  return df
