# airflow_exchange_rates

Простая чекалка курса валютной пары RUB-USD, RUB-AMD, RUB-EUR

Опрашивает 1 раз в день, результаты записывает в postgres

![изображение](https://user-images.githubusercontent.com/37380865/215271360-f730e9bb-e317-4581-a38b-833be7b76289.png)

## create_table_task 
проверяет наличие таблицы в БД, если её нет, то создает

## get_rate_BASE_CURRENCY 
забирает текущие курсы 

## insert_rate_BASE_CURRENCY 
инсертит курсы в БД

## make_report 
складывает курсы за сегодня в csv
