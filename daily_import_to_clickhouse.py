# Импорт библиотек
import numpy as np
import pandas as pd
import pandahouse as ph

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
    'start_date': datetime(2023, 5, 8),# Дата начала выполнения DAG
}

# Интервал запуска DAG (ежедневно,в 10:00)
schedule_interval = '0 10 * * *'

# Cоздание дага и тасок
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_a_nikonova_24():   
    
    
    # Число просмотров и лайков для каждого пользователя
    @task
    def extract_feed():
        query_1 = '''
                    SELECT
                    today()-1 AS event_date,
                    user_id,
                    gender,
                    age,
                    os,
                    countIf(action='like') AS likes,
                    countIf(action='view') AS views
                    FROM simulator_20230420.feed_actions
                    WHERE toDate(time)=today()-1
                    GROUP BY user_id, gender, age, os
                  '''
        feed_query = get_df(query=query_1)
        return feed_query
    
    # Полученные и отправленные сообщения, получатели писем от пользователя, отправители писем пользователю 
    @task
    def extract_message():
        query_2 = ''' WITH messages AS (SELECT
                                        user_id,
                                        messages_received,
                                        messages_sent
                                        FROM (SELECT
                                                reciever_id AS user_id,
                                                COUNT(*) AS messages_received
                                                FROM simulator_20230420.message_actions
                                                WHERE toDate(time)=today()-1
                                                GROUP BY reciever_id) t_1
                                        FULL JOIN (SELECT
                                                user_id,
                                                COUNT(reciever_id) AS messages_sent
                                                FROM simulator_20230420.message_actions
                                                WHERE toDate(time)=today()-1
                                                GROUP BY user_id)t_2
                                                USING user_id),
                            users AS (SELECT 
                                        CASE
                                        WHEN user_id > 0 THEN user_id
                                        ELSE  reciever_id
                                        END user_id,
                                        users_sent,
                                        users_received
                                        FROM (SELECT
                                              user_id,
                                              COUNT(DISTINCT reciever_id) AS users_sent
                                              FROM simulator_20230420.message_actions
                                              WHERE toDate(time)=today()-1
                                              GROUP BY user_id) t1
                                              FULL JOIN
                                              (SELECT
                                              reciever_id AS user_id,
                                              reciever_id,
                                              users_received
                                              FROM
                                                (SELECT
                                                 reciever_id,
                                                 COUNT(DISTINCT user_id) AS users_received
                                                 FROM simulator_20230420.message_actions
                                                 WHERE toDate(time)=today()-1
                                                 GROUP BY reciever_id)) t2
                                             ON (t1.user_id=t2.user_id)),
                            about_users AS ( SELECT
                                            DISTINCT user_id,
                                            gender,
                                            age,
                                            os
                                            FROM
                                                (SELECT
                                                DISTINCT user_id,
                                                gender,
                                                age,
                                                os
                                                FROM simulator_20230420.message_actions
                                                WHERE user_id IN (SELECT user_id FROM messages)  OR user_id IN(SELECT user_id FROM users)
                                                UNION ALL
                                                SELECT
                                                DISTINCT  user_id,
                                                gender,
                                                age,
                                                os
                                                FROM simulator_20230420.feed_actions
                                                WHERE user_id IN (SELECT user_id FROM messages)  OR user_id IN(SELECT user_id FROM users)))
                            SELECT
                            today()-1 AS event_date,
                            messages.user_id AS user_id,
                            gender,
                            age,
                            os,
                            messages_received,
                            messages_sent,
                            users_sent,
                            users_received
                            FROM messages 
                            FULL JOIN users ON messages.user_id = users.user_id
                            FULL JOIN about_users ON messages.user_id = about_users.user_id
                                              '''
        message_query = get_df(query=query_2)
        return message_query
  
    # Джоин таблиц
    @task
    def join_queries(feed_query, message_query):
        full_tab = feed_query.merge(message_query,how='outer',on = ['event_date','user_id','gender','age','os']).fillna(0)
        # Разбивка пользователей на группы по возрасту вместо численного показателя
        def age_group_add(age):
            group =''
            if age < 21:
                group ='0-20'
            elif 21 <= age < 31:
                group ='21-30'
            elif 31 <= age < 41:
                group ='31-40' 
            elif 41 <= age < 51:
                group ='41-50' 
            elif 51 <= age < 61:
                group ='51-60'
            elif age >60:
                group ='> 60'
            else: 
                group ='unknown'
            return group
        full_tab['age']=full_tab['age'].apply(lambda x:age_group_add(x))
        return full_tab
    
    #Работа с объединенной таблицей:
    
    # Срез по полу
    @task
    def get_gender_dimension(full_tab):
        gender_dimension =  full_tab.groupby(['event_date','gender'],as_index = False)\
                                    .agg({'likes':'sum','views':'sum','messages_received':'sum',
                                          'messages_sent':'sum','users_sent':'sum','users_received':'sum'})\
                                    .rename(columns={'gender':'dimension_value'})
        gender_dimension.insert(1,'dimension','gender')
        return gender_dimension
    
    # Срез по возрасту
    @task
    def get_age_dimension(full_tab):
        age_dimension = full_tab.groupby(['event_date','age'],as_index = False)\
                                .agg({'likes':'sum','views':'sum','messages_received':'sum',
                                      'messages_sent':'sum','users_sent':'sum','users_received':'sum'})\
                                .rename(columns={'age':'dimension_value'})
        age_dimension.insert(1,'dimension','age')
        return age_dimension
    
    # Срез по операционной системе
    @task
    def get_os_dimension(full_tab):
        os_dimension = full_tab.groupby(['event_date','os'], as_index = False)\
                                .agg({'likes':'sum','views':'sum','messages_received':'sum',
                                      'messages_sent':'sum','users_sent':'sum','users_received':'sum'})\
                                .rename(columns={'os':'dimension_value'})
        os_dimension.insert(1,'dimension','os')
        return os_dimension
    
    # Создание финальной таблицы
    @task
    def get_result_tab(gender_dimension,age_dimension,os_dimension):
        result_tab = pd.concat([gender_dimension,age_dimension,os_dimension], join='outer', axis =0)
        # Формат данных должен совпадать с форматом в таблице sql 
        result_tab = result_tab.astype({'event_date': 'datetime64', 'dimension': 'str', 'dimension_value' : 'str', 
                   'likes' : 'int', 'views' : 'int', 'messages_received' : 'int',
                   'messages_sent' : 'int', 'users_sent' : 'int','users_received' : 'int'})
        return result_tab
    
    # Загрузка финальной таблицы в схему test
    @task
    def load_table(result_tab):
        con_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'}

        create_tab = '''CREATE TABLE IF NOT EXISTS test.a_nikonova_24
                         (event_date DATE,
                         dimension VARCHAR,
                         dimension_value VARCHAR,
                         views INT,
                         likes INT,
                         messages_received INT,
                         messages_sent INT,
                         users_sent INT,
                         users_received INT
                         ) ENGINE = MergeTree
                         ORDER BY event_date;
         '''
        ph.execute(query=create_tab, connection=con_test)
        ph.to_clickhouse(df=result_tab, table='a_nikonova_24', connection=con_test, index=False)
    
    # Выполнение тасок
    
    # Extract
    feed_query = extract_feed()
    message_query =  extract_message()
    
    # Transform
    full_tab = join_queries(feed_query, message_query)
    gender_dimension = get_gender_dimension(full_tab)
    age_dimension = get_age_dimension(full_tab)
    os_dimension = get_os_dimension(full_tab)
    result_tab = get_result_tab(gender_dimension,age_dimension,os_dimension)
    
    # Load
    load_table(result_tab)
    
dag_a_nikonova_24 = dag_a_nikonova_24()  