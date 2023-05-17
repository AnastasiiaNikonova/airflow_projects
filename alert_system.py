# Импорт библиотек

# работа в питоне:
import numpy as np
import pandas as pd
from scipy import stats
# обработка запросов:
import pandahouse as ph
# Построение графиков:
import seaborn as sns
import matplotlib.pyplot as plt
import io
# airflow и телеграм бот:
import telegram
from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta
import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Подключение к базе данных и запись запроса в датафрейм:
def get_df(query):
    connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20230420',
                      'user':'student', 
                      'password':'dpo_python_2020'}

    df = ph.read_clickhouse(query, connection=connection)
    return df


# Алгоритм поиска аномалий на основе межквартильного размаха
def check_anomaly(df,metric,a=4,sample_size = 6):
    
    # Расчет 25 и 75 процентиля,межквартильного размаха, верхней и нижней границ доверительного интервала 
    df['q25'] = df[metric].shift(1).rolling(sample_size).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(sample_size).quantile(0.75)
    df['iqr'] = df['q75']-df['q25']
    df['up']  = df['q75'] +a*df['iqr']
    df['low'] = df['q25'] -a*df['iqr']
    
    # Сглаживание границ доверительных интервалов    
    df['up'] = df['up'].rolling(sample_size, center = True,min_periods=1).mean()
    df['low'] = df['low'].rolling(sample_size, center = True,min_periods=1).mean()
    
    # Определение вхождения значения метрики за последнюю 15-минутку в доверительный интервал, создание алерта в случае невхождения
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = True
    else:
        is_alert = False
        
    return is_alert, df


# Параметры  DAG
default_args = {
    'owner': 'a_nikonova_24',# Владелец операции
    'depends_on_past': False,# Зависимость от прошлых запусков
    'retries': 2,# Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5),# Промежуток между перезапусками
    'start_date': datetime(2023, 5, 16),# Дата начала выполнения DAG
}
# Интервал запуска DAG каждые 15 минут
schedule_interval = '*/15 *  * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alert_system_nikonova():
    
    @task
    # система алертов
    def run_alerts():
        #Данные для работы бота
        
        chat_id = '-943174102' 
        my_token = '5840058201:AAHexf9KArdrE_aug81erh6yM5_KIJRsMOc'
        bot = telegram.Bot(token=my_token)
       
        # Запрос со всеми необходимыми матриками, запись в общий датафрейм
        query = f'''
                WITH feed AS (SELECT
                    toStartOfFifteenMinutes(time) AS quart_hour,
                    COUNT(DISTINCT user_id) AS feed_users,
                    countIf(action = 'like') AS likes,
                    countIf(action = 'view') AS views,
                    countIf(action = 'like')/countIf(action = 'view') AS ctr
                    FROM simulator_20230420.feed_actions
                    WHERE time >= yesterday() AND time < toStartOfFifteenMinutes(now())
                    GROUP BY toStartOfFifteenMinutes(time)),
                msg AS (SELECT
                    toStartOfFifteenMinutes(time) AS quart_hour,
                    COUNT(DISTINCT user_id) AS msg_users,
                    COUNT(user_id) AS messages
                    FROM simulator_20230420.message_actions
                    WHERE time >= yesterday() AND time < toStartOfFifteenMinutes(now())
                    GROUP BY toStartOfFifteenMinutes(time))

                SELECT
                quart_hour,
                feed_users,
                msg_users,
                likes,
                views,
                messages
                FROM feed JOIN msg USING quart_hour
                ORDER BY quart_hour 
                '''     
        data = get_df(query) 
    
        # интересующие метрики (названы как колонки в df)
        metrics = ['feed_users', 'msg_users', 'likes', 'views', 'messages']
    
        # Создание датафрейма для каждой отдельной метрики, проверка на аномалии
        for metric in metrics:        
            df = data[['quart_hour',metric]].copy()
            is_alert, df = check_anomaly(df,metric)
        
            if is_alert:
                message = f'''Метрика {metric}:\nтекущее значение: {df[metric].iloc[-1]}\nотклонение от предыдущего значения: {abs(np.round((1 - df[metric].iloc[-1]/df[metric].iloc[-2])*100,2))}%\nдашборд: https://superset.lab.karpov.courses/superset/dashboard/3476/
                '''
                    
                # График метрики
                sns.set(rc = {'figure.figsize':(16, 10)})
                plt.tight_layout()
                ax = sns.lineplot(data = df, x ='quart_hour', y = metric, linewidth=3, color = 'royalblue', label = 'metric')
                ax = sns.lineplot(data = df, x ='quart_hour', y = 'up',  color = 'seagreen', label = 'up')
                ax = sns.lineplot(data = df, x ='quart_hour', y = 'low', color = 'sandybrown',label = 'low')
                ax.set(ylim=(0,None))
                ax.set_facecolor("whitesmoke")
    
                ax.set_title(metric, fontsize=25)
                plt.xlabel('time', fontsize=16)
                plt.ylabel(None, fontsize=16)
        
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = f'{metric}.png'
    
                plt.close()    
                    
                # Отправление сообщения об алерте и графика в чат         
                bot.sendMessage(chat_id=chat_id, text=message)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
   
    run_alerts()
alert_system_nikonova = alert_system_nikonova()
