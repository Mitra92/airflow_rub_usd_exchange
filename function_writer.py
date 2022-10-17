def write():
    import psycopg as psg
    from datetime import date
    import os
    import pandas as pd

    today = date.today()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    report_path = dir_path + f'/sql_response/Report_{today}.csv'

    with open(report_path, mode="w+") as f:
        connect_to = psg.connect(user='airflow',
                                 password='airflow', host='172.18.0.1',
                                 port=15432)
        curr = connect_to.cursor()
        curr.execute(f"SELECT * FROM public.exchange_rates WHERE base in ('USD', 'EUR', 'AMD') AND date = '{today}'")
        response = curr.fetchall()
        df = pd.DataFrame(response)
        df.to_csv(f)




