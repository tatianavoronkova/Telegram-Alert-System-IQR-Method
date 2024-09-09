import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import sys
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Настройки подключения к БД
connection = {
    'host': os.getenv('HOST_NAME'),
    'database': os.getenv('DATABASE'),
    'user': os.getenv('USER_NAME'),
    'password': os.getenv('PASSWORD')
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': os.getenv('OWNER_AIR'),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 7, 6),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

# зададим параметры бота
my_token = os.getenv('TG_TOKEN')
default_chat = os.getenv('TG_CHAT')
work_chat = os.getenv('TG_WORK_CHAT')

def check_anomaly(df, metric, a=3, n=5):
    # поиск аномалий методом межквартильного размаха
    df['q25'] = df[metric].shift(1).rolling(5).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(5).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_anomaly_tvoronkova():

    @task()
    def run_alert(chat=None):
        # система алертов
        chat_id = chat or default_chat
        bot = telegram.Bot(token=my_token) # получаем доступ

        # расчет метрик за предыдущий день
        q = """
            SELECT toStartOfFifteenMinutes(time) as ts, toDate(ts) as date, formatDateTime(ts, '%R') as hm, uniqExact(user_id) as users_qty, countIf(action='view') as views, countIf(action='like') as likes, likes/views as ctr
            FROM simulator_20240520.feed_actions
            WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts
            """

        data = ph.read_clickhouse(q, connection=connection)

        metrics_list = ['users_qty', 'views', 'likes', 'ctr']

        for metric in metrics_list:
            #print(metric)
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly(df, metric)
        
            if is_alert == 1:
                msg = '''Метрика {metric}:\nтекущее значение = {current_value:.2f}\nотклонение от предыдущего значения {prev_value:.2%}'''.format(metric=metric, current_value=df[metric].iloc[-1], prev_value=(df[metric].iloc[-1]/df[metric].iloc[-2]-1))

                sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
                plt.tight_layout()
                ax = sns.lineplot(x = df['ts'], y=df[metric], label=metric)
                ax = sns.lineplot(x = df['ts'], y=df['up'], label='up')
                ax = sns.lineplot(x = df['ts'], y=df['low'], label='low')

                for ind, label in enumerate(ax.get_xticklabels()): # этот цикл нужен чтобы разрядить подписи координат по оси Х,
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                ax.set(xlabel='time') # задаем имя оси Х
                ax.set(ylabel=metric) # задаем имя оси У

                ax.set_title('{}'.format(metric)) # задае заголовок графика
                ax.set(ylim=(0, None)) # задаем лимит для оси У

                # формируем файловый объект
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()

                # отправляем алерт
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    run_alert(-work_chat)
    
dag_anomaly_tvoronkova = dag_anomaly_tvoronkova()
