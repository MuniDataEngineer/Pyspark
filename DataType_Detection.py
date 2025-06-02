import re
from datetime import datetime

def is_bool(val: str) -> bool:
    return str(val).strip().lower() in ["true", "false", "yes", "no", "0", "1"]

def is_int(val: str) -> bool:
    return bool(re.fullmatch(r"-?\d+", val.strip()))

def is_float(val: str) -> bool:
    val = val.strip()
    # Ensure it's not a date
    if is_date(val) or is_timestamp(val):
        return False
    return bool(re.fullmatch(r"-?\d+\.\d+", val))

def is_date(val: str) -> bool:
    formats = ["%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"]
    return try_parse_formats(val.strip(), formats)

def is_timestamp(val: str) -> bool:
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f"
    ]
    return try_parse_formats(val.strip(), formats)

def is_null(val: str) -> bool:
    return str(val).strip().lower() in ["", "null", "none", "nan"]



def try_parse_formats(val: str, formats: list) -> bool:
    for fmt in formats:
        try:
            datetime.strptime(val, fmt)
            return True
        except:
            continue
    return False



def detect_type(val: str) -> str:
    val = str(val).strip()

    if is_null(val):
        return "string"
    if is_timestamp(val):
        return "timestamp"
    if is_date(val):
        return "date"
    if is_bool(val):
        return "boolean"
    if is_int(val):
        return "int"
    if is_float(val):
        return "float"

    return "string"

