# airflow_exchange_rates

Простая чекалка курса валютной пары RUB-USD, RUB-AMD, RUB-EUR

Опрашивает 1 раз в день, результаты записывает в postgres

![image](https://user-images.githubusercontent.com/37380865/197357102-15e59257-c4b4-4506-93e2-8584561e0786.png)

#create_table_task - проверяет наличие таблицы в БД, если её нет, то создает

#get_rate_BASE_CURRENCY - забирает текущие курсы 

#insert_rate_BASE_CURRENCY - инсертит курсы в БД

#make_report - складывает курсы за сегодня в csv
