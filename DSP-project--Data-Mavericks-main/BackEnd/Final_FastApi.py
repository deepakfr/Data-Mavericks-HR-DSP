from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict, Union
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import pyodbc
import pandas as pd
import os
import joblib
import numpy as np
import datetime as dt

load_dotenv()

app = FastAPI()

model = joblib.load('Model/model.joblib')
encoder = joblib.load('Model/one_hot_encoder.joblib')
scale = joblib.load('Model/scaler.joblib')

feat_list = ['training_hours', 'city_development_index', 'gender']
cat_feature = ['gender']
num_feature = ['training_hours', 'city_development_index']
targ = ['target']

class DatabaseHelper:
    @staticmethod
    def create_conn_string():
        server_details = os.getenv('SERVER')
        db_details = os.getenv('DATABASE')
        user_details = os.getenv('ADMINLOGIN')
        pass_details = os.getenv('PASSWORD')
        db_driver= '{ODBC Driver 17 for SQL Server}'
        
        return f"DRIVER={db_driver};SERVER={server_details};DATABASE={db_details};UID={user_details};PWD={pass_details};"

    @staticmethod
    def establish_connection(conn_string):
        return pyodbc.connect(conn_string)

    @staticmethod
    def execute_read_query(command, conn):
        return pd.read_sql(command, conn)

    @staticmethod
    def execute_write_query(command, conn):
        cur = conn.cursor()
        cur.execute(command)
        cur.commit()

    @staticmethod
    def format_query(table_name, data):
        column_str = ', '.join(data.keys())
        values_str = ', '.join(f"'{value}'" if isinstance(value, str) else str(value) for value in data.values())
        return f'INSERT INTO {table_name} ({column_str}) VALUES ({values_str});'

@app.post("/predict")
async def predict(data: List[Dict[str, Union[str, int, float]]]):
    df = pd.DataFrame(data)
    curr_date = dt.date.today().strftime("%d-%m-%Y")

    scale.fit(df[num_feature])
    num_scaled = scale.transform(df[num_feature])
    num_scaled_df = pd.DataFrame(data=num_scaled, columns=num_feature)

    encoder.fit(df[cat_feature])
    cat_encoded = encoder.transform(df[cat_feature])
    cat_encoded_df = pd.DataFrame.sparse.from_spmatrix(
        data=cat_encoded, columns=encoder.get_feature_names_out()
    )

    final_df = num_scaled_df.join(cat_encoded_df)
    predictions = model.predict(final_df)

    results = []
        
    for item, pred in zip(data, predictions.tolist()):
        item['prediction_date'] = str(curr_date)
        item['target'] = pred

        relevant_features = ['training_hours', 'city_development_index', 'gender', 'prediction_date', 'target']
        item = {k: v for k, v in item.items() if k in relevant_features}
        results.append(item)

        db_query = DatabaseHelper.format_query('HRJobs', item)
        db_conn_string = DatabaseHelper.create_conn_string()
        db_conn = DatabaseHelper.establish_connection(db_conn_string)
        try:
            DatabaseHelper.execute_write_query(db_query, db_conn)
        except Exception as e:
            return {"error": str(e)}

        db_conn.close()

    return results

@app.get("/past_predictions")
async def get_past_predictions(start: str = '', end: str = ''):
    query = "SELECT * FROM HRJob"
    if start and end:
        query = f"SELECT * FROM HRJob AS hr WHERE CONVERT(date, hr.prediction_date, 105) BETWEEN \'{start}\' AND \'{end}\'"

    db_conn_string = DatabaseHelper.create_conn_string()
    db_conn = DatabaseHelper.establish_connection(db_conn_string)
    past_preds = DatabaseHelper.execute_read_query(query, db_conn)
    past_preds_json = past_preds.to_json(orient="records")

    db_conn.close()

    return past_preds_json

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
