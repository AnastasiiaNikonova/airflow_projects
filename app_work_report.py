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


# Подключение к базе данных и запись запроса в датафрейм:
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
def app_work_report():
    
    @task
    def extract_all_users():
        # пользователи мессенджера, ленты и обоих продуктов, выборка за 7 дней
        query_1 ='''
        WITH feed_message AS (SELECT
                    toDate(time) AS date,
                    COUNT(DISTINCT user_id) AS feed_message_users
                    FROM simulator_20230420.feed_actions
                    WHERE user_id in (SELECT distinct user_id from simulator_20230420.message_actions
                                      WHERE toDate(time) BETWEEN yesterday()-6 AND yesterday())
                    AND toDate(time) BETWEEN yesterday()-6 AND yesterday()
                    GROUP BY toDate(time)
                    ORDER BY date),

        feed AS (SELECT
                    toDate(time) AS date,
                    COUNT(DISTINCT user_id) AS feed_users
                    FROM simulator_20230420.feed_actions 
                    WHERE toDate(time) BETWEEN yesterday()-6 AND yesterday()
                    GROUP BY toDate(time)
                    ORDER BY date),
        message AS (SELECT
                    toDate(time) AS date,
                    COUNT(DISTINCT user_id) AS message_users
                    FROM simulator_20230420.message_actions
                    WHERE toDate(time) BETWEEN yesterday()-6 AND yesterday()
                    GROUP BY toDate(time)
                    ORDER BY date)

        SELECT
        feed_message.date AS date,
        feed_users,
        message_users,
        feed_message_users
        FROM feed_message JOIN feed ON feed_message.date = feed.date
        JOIN message ON message.date = feed.date
        ORDER BY date DESC
        '''
        all_users = get_df(query_1)
        return all_users
   
    @task
    def extract_feed():
        # Основные метрики по ленте
        query_2 = '''
        SELECT
        date,
        ROUND(AVG(likes)) AS avg_likes,
        SUM(likes) AS sum_likes,
        ROUND(AVG(views)) AS avg_views,
        SUM(views) AS sum_views,
        ROUND(AVG(ctr),2) AS avg_ctr,
        ROUND(SUM(likes)/SUM(views)*100,2) AS sum_ctr
                FROM (SELECT
                toDate(time) AS date,
                user_id,
                countIf(action = 'like') AS likes,
                countIf(action = 'view') AS views,
                countIf(action = 'like')/countIf(action = 'view')*100 AS ctr
                FROM simulator_20230420.feed_actions
                WHERE toDate(time) BETWEEN yesterday()-6 AND yesterday()
                GROUP BY toDate(time),user_id)
        GROUP BY date
        ORDER BY date DESC
        '''
        feed = get_df(query_2)
        return feed
    
    @task
    def extract_msg():
        # данные по мессенджеру
        query_3 = '''
        WITH for_avg AS (SELECT
            date,
            ROUND(AVG(msg_send)) AS avg_msg_sent,
            ROUND(AVG(count_reciever)) AS avg_resivers_per_user
            FROM(
                SELECT
                toDate(time) AS date,
                user_id,
                COUNT(reciever_id) AS msg_send,
                COUNT(DISTINCT reciever_id) AS count_reciever
                FROM simulator_20230420.message_actions
                WHERE toDate(time) BETWEEN yesterday()-6 AND yesterday()
                GROUP BY toDate(time),user_id)
            GROUP BY date),

        for_count AS (SELECT
                toDate(time) AS date,
                COUNT(DISTINCT user_id) AS senders,
                COUNT(DISTINCT reciever_id) AS recievers,
                COUNT(user_id) AS msg_sent
                FROM simulator_20230420.message_actions
                WHERE toDate(time) BETWEEN yesterday()-6 AND yesterday()
                GROUP BY toDate(time))

        SELECT
        date,
        senders,
        recievers,
        msg_sent,
        avg_msg_sent,
        avg_resivers_per_user
        FROM for_avg JOIN for_count USING date
        ORDER BY date DESC
        '''
        msg = get_df(query_3)
        return msg
    @task
    def extract_new_users():
        query_4 =   '''
                   WITH feed_users AS (SELECT 
                                    DISTINCT user_id,
                                    min(toDate(time)) AS date
                                    FROM simulator_20230420.feed_actions
                                    GROUP BY user_id
                                    HAVING date >= today()-7 AND date < today()),

                    feed_new AS (SELECT
                                date,
                                COUNT(user_id) AS feed_new_users
                                FROM
                                feed_users
                                GROUP BY date),

                    msg_new AS  (SELECT
                                date,
                                COUNT(user_id) AS msg_new_users
                                FROM
                                (SELECT 
                                    DISTINCT user_id,
                                    min(toDate(time)) AS date
                                    FROM simulator_20230420.message_actions
                                    GROUP BY user_id
                                    HAVING date >= today()-7 AND date < today())
                                GROUP BY date ),

                    feed_msg_new AS (SELECT
                                date,
                                COUNT(user_id) AS feed_msg_new_users
                                FROM
                                 (SELECT 
                                    DISTINCT user_id,
                                    min(toDate(time)) AS date
                                    FROM simulator_20230420.message_actions
                                    GROUP BY user_id
                                    HAVING date >= today()-7 AND date < today())
                                WHERE user_id IN (SELECT user_id FROM feed_users) 
                                GROUP BY date )
                SELECT
                feed_new.date AS date,
                feed_new_users,
                msg_new_users,
                feed_msg_new_users
                FROM feed_new FULL JOIN msg_new ON(feed_new.date = msg_new.date)
                    FULL JOIN feed_msg_new ON(msg_new.date=feed_msg_new.date)
                ORDER BY date DESC
                    '''
        new_users = get_df(query_4)
        return new_users
    
    
    @task 
    def get_message(all_users,feed,msg,new_users):
        message = (f'''ОТЧЕТ ЗА {all_users.date.dt.date[0]}

    Число пользователей:
        ленты: {all_users.feed_users[0]} 
        мессенджера: {all_users.message_users[0]} 
        обоих продуктов: {all_users.feed_message_users[0]}

    Новые пользователи:
        ленты: {new_users.feed_new_users[0]} 
        мессенджера: {new_users.msg_new_users[0]} 
        обоих продуктов: {new_users.feed_msg_new_users[0]}

    _________________    
    Лента новостей:
        лайки: {feed.sum_likes[0]}
        просмотры: {feed.sum_views[0]}
        CTR: {feed.sum_ctr[0]}

    Средние показатели на пользователя:
        лайки: {feed.avg_likes[0]}
        просмотры: {feed.avg_views[0]}
        CTR: {feed.avg_ctr[0]}
    _________________
    Мессенджер:
        отправлено писем:{msg.msg_sent[0]}
        отправители: {msg.senders[0]}
        получатели: {msg.recievers[0]}

    Средние показатели на пользователя:
        отправлено писем: {msg.avg_msg_sent[0]}
        получатели: {msg.avg_resivers_per_user[0]}
    __________________
    ''')
        return message
    @task 
    def get_plot(all_users,feed,msg,new_users):
        plt.rcParams['font.size'] = '18'
        figure, ax = plt.subplots(8,2, figsize=(43, 40))
        sns.lineplot(data = all_users, x ='date', y='feed_users',color='royalblue', linewidth=3, ax=ax[0][0])\
                        .set(xlabel=None,ylabel=None,title = 'feed users')
        sns.lineplot(data = new_users, x ='date', y='feed_new_users',color='royalblue', linewidth=3, ax=ax[0][1])\
                        .set(xlabel=None,ylabel=None,title = 'new feed users')
        sns.lineplot(data = all_users, x = 'date', y='message_users',color='skyblue', linewidth=3, ax=ax[1][0])\
                        .set(xlabel=None,ylabel=None,title = 'message users')
        sns.lineplot(data = new_users, x ='date', y='msg_new_users',color='skyblue', linewidth=3,ax=ax[1][1])\
                        .set(xlabel=None,ylabel=None,title = 'new message users')
        sns.lineplot(data = all_users, x = 'date', y='feed_message_users',color='orchid', linewidth=3,  ax=ax[2][0])\
                        .set(xlabel=None,ylabel=None,title = 'feed and message users')
        sns.lineplot(data = new_users, x = 'date', y='feed_msg_new_users',color='orchid', linewidth=3,  ax=ax[2][1])\
                        .set(xlabel=None,ylabel=None,title = 'new feed and message users')

        sns.lineplot(data = feed, x ='date', y='sum_likes',color='olivedrab', linewidth=3, ax=ax[3][0])\
                        .set(xlabel=None,ylabel=None,title = 'likes')
        sns.lineplot(data = feed, x ='date', y='avg_likes',color='olivedrab', linewidth=3, ax=ax[3][1])\
                        .set(xlabel=None,ylabel=None,title = 'average likes')
        sns.lineplot(data = feed, x = 'date', y='sum_views',color='khaki', linewidth=3, ax=ax[4][0])\
                        .set(xlabel=None,ylabel=None,title = 'views')
        sns.lineplot(data = feed, x ='date', y='avg_views',color='khaki', linewidth=3,ax=ax[4][1])\
                        .set(xlabel=None,ylabel=None,title = 'average views')
        sns.lineplot(data = feed, x = 'date', y='sum_ctr',color='mediumaquamarine', linewidth=3,  ax=ax[5][0])\
                        .set(xlabel=None,ylabel=None,title = 'CTR')
        sns.lineplot(data = feed, x = 'date', y='avg_ctr',color='mediumaquamarine', linewidth=3,  ax=ax[5][1])\
                        .set(xlabel=None,ylabel=None,title = 'average CTR')

        sns.lineplot(data = msg, x ='date', y='senders',color='darkorange', linewidth=3, ax=ax[6][0])\
                        .set(xlabel=None,ylabel=None,title = 'senders')
        sns.lineplot(data = msg, x ='date', y='recievers',color='gold', linewidth=3, ax=ax[6][1])\
                        .set(xlabel=None,ylabel=None,title = 'recievers')
        sns.lineplot(data = msg, x = 'date', y='msg_sent',color='sienna', linewidth=3, ax=ax[7][0])\
                        .set(xlabel=None,ylabel=None,title = 'messages sent')
        sns.lineplot(data = msg, x ='date', y='avg_msg_sent',color='sienna', linewidth=3,ax=ax[7][1])\
                        .set(xlabel=None,ylabel=None,title = 'average messages sent')

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics.png'

        return plot_object  
    
    @task
    def load_report(message,plot_object):
        chat_id = '-992300980'
        my_token = '5840058201:AAHexf9KArdrE_aug81erh6yM5_KIJRsMOc'
        bot = telegram.Bot(token=my_token)
        
        bot.sendMessage(chat_id=chat_id, text=message)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    # Выполнение тасок
    
    # Extract
    all_users = extract_all_users()
    new_users = extract_new_users()
    feed      = extract_feed()
    msg       = extract_msg()
    
    # Transform
    message = get_message(all_users,feed,msg,new_users)
    plot_object = get_plot(all_users,feed,msg,new_users)
    # Load
    load_report(message,plot_object)
    
app_work_report = app_work_report()