from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from typing import Any
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class CurrencyScoopHook(BaseHook):
    def __init__(self, currency_conn_id: str):
        super().__init__()
        self.conn_id = currency_conn_id

    def get_rate(self, date, base_currency: str, currency: str):
        url = 'https://api.currencyscoop.com/v1/historical'
        params = {
            'base': base_currency.upper(),
            'symbols': currency.upper(),
            'api_key': self._get_api_key(),
            'date': str(date),
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()['response']['rates'][currency]

    def _get_api_key(self):
        conn = self.get_connection(self.conn_id)
        if not conn.password:
            raise AirflowException('Missing API key (password) in connection settings')
        return conn.password


class CurrencyScoopOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            base_currency: str,
            currency: str,
            conn_id: str = 'currency_conn_id',
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.base_currency = base_currency
        self.currency = currency

    def execute(self, context: Any):
        api = CurrencyScoopHook(self.conn_id)
        return api.get_rate(context['execution_date'].date(), self.base_currency, self.currency)

with DAG(
        dag_id='exchange_rates',
        start_date=datetime(2022, 10, 1),
        schedule_interval='@daily',
) as dag:

    create_table = PostgresOperator(
        task_id='create_table_task',
        sql='sql\create_table.sql',
        postgres_conn_id='postgres_default',
    )

    tasks = []

    for base, currency in [
        ('RUB', 'USD'),
        ('RUB', 'EUR'),
        ('RUB', 'AMD'),
        ('USD', 'RUB'),
        ('EUR', 'RUB'),
        ('AMD', 'RUB'),
    ]:
        get_rate_task = CurrencyScoopOperator(
            task_id=f'get_rate_{base}_{currency}',
            base_currency=base,
            currency=currency,
            conn_id='currency_conn_id',
            dag=dag,
            do_xcom_push=True,
        )

        insert_rate = PostgresOperator(
            task_id=f'insert_rate_{base}_{currency}',
            postgres_conn_id='postgres_default',
            sql='sql/insert_rate.sql',
            params={
                'base_currency': base,
                'currency': currency,
                'get_rate_task_id': f'get_rate_{base}_{currency}'
            }
        )

        get_rate_task >> insert_rate

        tasks.append(get_rate_task)

    create_table.set_downstream(tasks)
