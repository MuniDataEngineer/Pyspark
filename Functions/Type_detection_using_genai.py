#Field_Type_Detection

from google import genai
from google.genai import types
import ast

result = {}

def type_Detection(api_key,df):
  api_key = api_key
  data = df
  try:
    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(model="gemini-2.0-flash",contents="hi")
    try:
      response = client.models.generate_content(
          model="gemini-2.0-flash",
          config=types.GenerateContentConfig(
            system_instruction="You are a pyspark developer and know all the data and types."),
          contents=f"Read the dataframe {data}, predict the extact datatype, return only datatype and field as a python dictionary type and without ```python or any triple backticks."
      )
      result = ast.literal_eval(response.text)
    except:
        print("provide the valid dataframe")
  except:
    print("provide a valid api key")
  return result
