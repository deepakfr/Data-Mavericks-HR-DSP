import streamlit as st
import requests
import pandas as pd
import numpy as np

URL_PREDICT = 'http://backend:8000/predict'
URL_PAST_PREDICTIONS = 'http://backend:8000/past_predictions'

feature_categories = ['training_hours', 'city_development_index', 'gender']
cat_columns = ['gender']
num_columns = ['training_hours', 'city_development_index']
target_column = ['target']

def format_single_prediction_data(hours, cdi, gen):
    return {
        'training_hours': hours,
        'city_development_index': cdi,
        'gender': gen
    }

def format_bulk_prediction_data(csv_data):
    csv_data[cat_columns] = csv_data[cat_columns].fillna("nan")
    csv_data[num_columns] = csv_data[num_columns].fillna(0)
    csv_data[feature_categories] = csv_data[feature_categories].fillna(0)
    csv_data = csv_data.replace([np.inf, -np.inf], np.nan)
    return csv_data.to_dict(orient='records')

def prediction_page():
    st.header("Prediction Section")
    hours = st.number_input("Training Hours")
    cdi = st.number_input("City Development Index")
    gen = st.text_input("Gender")

    if st.button("Single Prediction"):
        data = format_single_prediction_data(hours, cdi, gen)
        response = requests.post(URL_PREDICT, json=[data])
        data = response.json()
        if response.status_code == 200:
            df = pd.DataFrame(data)
            st.table(df)
        else:
            st.write("Error occurred:", response.status_code)

    st.subheader("Bulk Prediction")
    csv_file = st.file_uploader("Upload CSV file", type=["csv"])

    if csv_file is not None:
        csv_data = pd.read_csv(csv_file)
        data_dict = format_bulk_prediction_data(csv_data)
        if st.button("Bulk Prediction"):
            response = requests.post(URL_PREDICT, json=data_dict)
            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data)
                st.table(df)
            else:
                st.write("Error occurred:", response.status_code)

def past_predictions_page():
    
    st.header("Past Predictions")
    start_date = st.date_input("Start Date")
    end_date = st.date_input("End Date")

    if st.button("Get Past Predictions"):
        params = {
            'start': start_date.strftime("%d-%m-%Y"),
            'end': end_date.strftime("%d-%m-%Y")
        }
        try:
            response = requests.get(URL_PAST_PREDICTIONS, params=params)
            response.raise_for_status()  # This will raise an exception for 4xx/5xx responses
        except requests.exceptions.HTTPError as err:
            st.write(f"HTTP error occurred: {err}")
        except requests.exceptions.RequestException as err:
            st.write(f"Error occurred: {err}")
        else:
            past_predictions = response.json()
            if past_predictions:
                st.write("Past Predictions:")
                past_predictions_df = pd.DataFrame(past_predictions)
                st.dataframe(past_predictions_df)
            else:
                st.write("No past predictions found.")


def main():
    st.set_page_config(page_title='HR Prediction App')
    st.title("HR Predictions")
    st.write('Provide the details for a single prediction or upload a CSV file for bulk predictions.')
    options = ["Predict", "Past Predictions"]
    choice = st.sidebar.selectbox("Select Option", options)
    if choice == "Predict":
        prediction_page()
    elif choice == "Past Predictions":
        past_predictions_page()

if __name__ == "__main__":
    main()

