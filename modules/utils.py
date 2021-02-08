import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable


def download_titanic_dataset() -> dict:
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    return df.to_json()

def pivot_dataset(**kwargs) -> dict:
    ti = kwargs['ti']
    jsn = ti.xcom_pull(key=None, task_ids='download_titanic_dataset')
    titanic_df = pd.read_json(jsn)
    return titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index().to_json()

def mean_fare_per_class(**kwargs) -> dict:
    ti = kwargs['ti']
    jsn = ti.xcom_pull(key=None, task_ids='download_titanic_dataset')
    titanic_df = pd.read_json(jsn)
    return titanic_df.groupby('Pclass').Fare.mean().reset_index().to_json()


def pivot_to_db(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    ti = kwargs['ti']
    jsn = ti.xcom_pull(key=None, task_ids='pivot_titanic_dataset')
    df = pd.read_json(jsn)

    tbl_name = Variable.get('TABLE_TITANIC_PIVOT')
    for idx, row in df.iterrows():
        db_query = 'insert into ' + tbl_name + ' (sex, 1_count, 2_count, 3_count) values (%s, %s, %s, %s)'
        pg_hook.run(db_query, parameters=(row['Sex'], row[1], row[2], row[3],))


def mean_fare_to_db(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    ti = kwargs['ti']
    jsn = ti.xcom_pull(key=None, task_ids='mean_fare_titanic_dataset')
    df = pd.read_json(jsn)

    tbl_name = Variable.get('TABLE_TITANIC_MEAN_FARE')
    for idx, row in df.iterrows():
        db_query = 'insert into ' + tbl_name + ' (pclass, mean_fare) values (%s, %s)'
        pg_hook.run(db_query, parameters=(row['Pclass'], row['Fare'],))