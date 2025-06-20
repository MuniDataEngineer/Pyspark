import re

def fieldNameValidation(fields):
  new_cols = [re.sub(r"[^a-zA-Z0-9_\s]", "", col).strip().lower().replace(" ","_") for col in fields]
  return new_cols  
