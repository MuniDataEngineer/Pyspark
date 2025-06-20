# PySpark JSON Handler – Flatten & Field Name Cleaner

This project provides a PySpark-based utility to read, normalize, and flatten complex JSON files. It also includes robust field name validation that removes or replaces problematic characters, making the data safe for further processing.

---

## 🔧 Features

- ✅ Reads deeply nested JSON with `struct` and `array` types
- ✅ Recursively flattens arrays and nested objects
- ✅ Automatically handles empty or missing nested fields
- ✅ Validates and cleans column names:
  - Removes or replaces special characters (e.g., `@`, `#`, `-`, `$`)
  - Converts names to lowercase
  - Replaces spaces with underscores

---

## ▶️ How to Run 
⚠️ **Important:** Run this on the notebook in Google Colab for the best experience.
1. Clone the repository:
`!git clone https://github.com/MuniDataEngineer/Pyspark.git`
2. Run the main.py file:
`%run /content/Pyspark/Json_Handler/main.py`

----

🌐Colab
🔗 https://colab.research.google.com/
