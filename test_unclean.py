import sys, os
sys.path.append('c:/jiya/big-data-scoring/DataTrustFramework')
import pandas as pd
from dashboard.app import process_in_spark, app

with app.app_context():
    try:
        res = process_in_spark("c:/jiya/big-data-scoring/uploads/uncleaned_data.csv")
        print("\n\nSUCCESS:")
        print(res)
    except Exception as e:
        print("\n\nEXCEPTION:")
        print(str(e))
