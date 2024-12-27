import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from sklearn.metrics import r2_score
from main import execute_sql_query
import seaborn as sns
import json

import logging
from main import execute_sql_query

logging.basicConfig(level=logging.DEBUG)

# Load query parameters from JSON
try:
    with open('query_params.json', 'r') as file:
        query_params = json.load(file)
except Exception as e:
    logging.error(f"Error loading query parameters: {e}")
    raise

try:
    # Construct the SQL query dynamically
    logging.debug("Executing SQL query...")
    query = f"""
        SELECT * FROM DP_CDR_Data 
        WHERE dp_date BETWEEN '{query_params['start_date']}' AND '{query_params['end_date']}'
    """
    df = execute_sql_query(query, database_name="RawData")
    logging.debug(f"Query executed successfully. Dataframe shape: {df.shape}")
    print(df.head())
except Exception as e:
    logging.error(f"Error occurred: {e}")
    raise


def summary(df):
    '''Summary Statistics on Data'''
    print("Summary Statistics:", df.describe())


def sum_data(df):
    '''function to find sum of data'''
    df = df.groupby(['DP_DATE'])[['DP_MOC_COUNT',
                                  'DP_MOC_DURATION',
                                  'DP_MTC_COUNT',
                                  'DP_MTC_DURATION',
                                  'DP_MOSMS_COUNT',
                                  'DP_MTSMS_COUNT',
                                  'DP_DATA_COUNT',
                                  'DP_DATA_VOLUME']].sum()
    return df


def mean_data(df):
    df = df.groupby('DP_DATE')[['DP_MOC_COUNT',
                                'DP_MOC_DURATION',
                                'DP_MTC_COUNT',
                                'DP_MTC_DURATION',
                                'DP_MOSMS_COUNT',
                                'DP_MTSMS_COUNT',
                                'DP_DATA_COUNT',
                                'DP_DATA_VOLUME']].mean().reset_index()
    return df

'''r2 value calculation for most significant feature'''
def r2(x, y, **kwargs):
    r2_val = r2_score(x, y)
    print(f'R^2 = {r2_val: .2f}')
    ax = plt.gca()  # Corrected to assign ax properly
    ax.text(0.05, 0.95, f'R2 = {r2_val:.2f}', transform=ax.transAxes, fontsize=12, verticalalignment='top')


# Check and convert column types to numeric if necessary
for col in df.columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')


columns_to_compare = df.columns


columns_to_compare = df.dropna(axis=1, how='all').columns
'''Plotting R2 for all features in dataset'''
for col in columns_to_compare:
    if col != 'PSEUDO_CHURNED':

        if df[col].isnull().all():
            print(f"Skipping column {col} because it contains only NaN values")
            continue


        data_for_plot = df[[col, 'PSEUDO_CHURNED']].dropna()

        if len(data_for_plot) <= 1:
            print(f"Skipping column {col} because it does not have enough valid data for plotting")
            continue

        g = sns.jointplot(x=col, y='PSEUDO_CHURNED', data=data_for_plot, kind="reg")

        x = data_for_plot[col]
        y = data_for_plot['PSEUDO_CHURNED']

        r2_val = r2_score(x, y)


        plt.gca().text(0.05, 0.95, f'R2 = {r2_val:.2f}', transform=plt.gca().transAxes, fontsize=12,
                       verticalalignment='top')

        plt.savefig(
            f'./output/{col}_vs_PSEUDO_CHURNED')  # Fixed save path
        plt.show()

        plt.close()

# summary = summary(df)
# # sum_data =  sum_data(df)
# sum_date = sum_data(df)
# print(sum_date)
#
# # fig, ax = plt.subplots(figsize=(10, 6))
# plt.plot(sum_date['DP_DATA_COUNT'], sum_date['DP_DATA_VOLUME'])
# # ax.plot(sum_date['DP_DATE'], sum_date[[i for i in sum_date if i != 'DP_DATE']])
# plt.legend()
# plt.show()

# summary = summary(df)
# # sum_data =  sum_data(df)
# sum_date = sum_data(df)
# print(sum_date)
#
# # fig, ax = plt.subplots(figsize=(10, 6))
# plt.plot(sum_date['DP_DATA_COUNT'], sum_date['DP_DATA_VOLUME'])
# # ax.plot(sum_date['DP_DATE'], sum_date[[i for i in sum_date if i != 'DP_DATE']])
# plt.legend()
# plt.show()