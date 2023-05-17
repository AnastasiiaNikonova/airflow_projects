# Импорт библиотек
import numpy as np
import pandas as pd
import pandahouse as ph
import telegram

import matplotlib.pyplot as plt
import seaborn as sns
import io

from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Функция для обработки SQL запроса и получения датафрейма
def get_df(query):
    connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230420',
                      'user':'student', 
                      'password':'dpo_python_2020'}

    df = ph.read_clickhouse(query, connection=connection)
    return df

# Параметры  DAG
default_args = {
    'owner': 'a_nikonova_24',# Владелец операции
    'depends_on_past': False,# Зависимость от прошлых запусков
    'retries': 2,# Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5),# Промежуток между перезапусками
    'start_date': datetime(2023, 5, 10),# Дата начала выполнения DAG
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def daily_report_nikonova():
    
    #задаем бота и чат
    @task()
    def get_report(chat=None):
        chat_id = chat or '-992300980'
        my_token = '5840058201:AAHexf9KArdrE_aug81erh6yM5_KIJRsMOc'
        bot = telegram.Bot(token=my_token)
    
   # SQL запрос согласно ТЗ
        query = '''
        SELECT
        toDate(time) AS date,
        COUNT(DISTINCT user_id) AS DAU,
        countIf(action = 'view') AS views,
        countIf(action = 'like') AS likes,
        countIf(action = 'like')/countIf(action = 'view') AS CTR
        FROM simulator_20230420.feed_actions
        WHERE toDate(time) BETWEEN yesterday()-6 and yesterday()
        group by toDate(time)
        ORDER BY date DESC
        '''
    # Датафрейм для работы в python
        for_bot = get_df(query)
        
    # Отправка текстового сообщения,отчет за вчера
        message =f'''
                    отчет "ЛЕНТА НОВОСТЕЙ"
                    за {for_bot.date.dt.date[0]}:

                    DAU: {for_bot.DAU[0]}
                    Просмотры: {for_bot.views[0]}
                    Лайки: {for_bot.likes[0]}
                    CTR: {for_bot.CTR[0].round(2)}
                    ''' 
        bot.sendMessage(chat_id=chat_id, text=message)
        
    # Отправка изображения, отчет за неделю
        figure, ax = plt.subplots(4,figsize=(15, 20))
        sns.lineplot(x=for_bot.date, y=for_bot.DAU, color='royalblue', linewidth=3, ax=ax[0])\
            .set(xlabel=None,ylabel=None,title = 'DAU')
        sns.lineplot(x=for_bot.date, y=for_bot.views, color='darkseagreen', linewidth=3, ax=ax[1])\
            .set(xlabel=None,ylabel=None,title = 'VIEWS')
        sns.lineplot(x=for_bot.date, y=for_bot.likes, color='mediumvioletred', linewidth=3, ax=ax[2])\
            .set(xlabel=None,ylabel=None,title = 'LIKES')
        sns.lineplot(x=for_bot.date, y=for_bot.CTR, color='darkorange', linewidth=3, ax=ax[3])\
            .set(xlabel=None,ylabel=None,title = 'CTR')

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'weekly_dynamics.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)      
    
    get_report()

daily_report_nikonova = daily_report_nikonova()