import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse
from datetime import date
import io
import sys
import os






metrics = ['users_lenta', 'views', 'likes', 'CTR', 'users_message', 'messages']

def check_anomaly(df, metric, threshold):
    current_ts = df['ts'].max()  
    day_ago_ts = current_ts - pd.DateOffset(days=1)  

    current_value = df[df['ts'] == current_ts][metric].iloc[0] 
    day_ago_value = df[df['ts'] == day_ago_ts][metric].iloc[0] 

    
    if current_value <= day_ago_value:
        diff = abs(current_value / day_ago_value - 1)
    else:
        diff = abs(day_ago_value / current_value - 1)

    
    if diff > threshold:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, current_value, diff


def run_alerts(chat=None):
    chat_id = chat or ...
    bot = telegram.Bot(token='...')

    
    q_1 = ''' with t1
as (SELECT toStartOfFifteenMinutes(time) as m_ts, count(DISTINCT user_id) as users_message, count(user_id) as messages
FROM simulator_20220120.message_actions 
group by m_ts),
t2
as 
(SELECT toStartOfFifteenMinutes(time) as f_ts, count(DISTINCT user_id) as users_lenta, countIf(user_id, action= 'like') as likes, countIf(user_id, action='view') as views,
ROUND(countIf(user_id, action= 'like')/countIf(user_id, action='view')*100,2) as CTR
FROM simulator_20220120.feed_actions 
group by f_ts)

SELECT f_ts as ts, toDate(f_ts) as date, formatDateTime(f_ts, '%R') as hm, users_lenta, likes, views, CTR, users_message, messages
from t1 join t2
on t1.m_ts = t2.f_ts
WHERE f_ts >=  today() - 1 and f_ts < toStartOfFifteenMinutes(now())
order by ts'''
    
    data = pandahouse.read_clickhouse(q_1, connection=connection)  


    for metric in metrics:
    
        is_alert, current_value, diff = check_anomaly(data, metric, threshold=0.3) 
    
        if is_alert:
            
            if metric == 'users_lenta':
                m_type = 'ленты новостей'
                link = 'http://superset.lab.karpov.courses/r/490'
                duty ='@R_Fisher'
            elif metric == 'views':
                m_type = 'ленты новостей'
                link = 'http://superset.lab.karpov.courses/r/491'
                duty = '@K_Pearson'
            elif metric == 'likes':
                m_type = 'ленты новостей'
                link = 'http://superset.lab.karpov.courses/r/492'
                duty = '@K_Pearson'    
            elif metric == 'CTR':
                m_type = 'ленты новостей'
                link = 'http://superset.lab.karpov.courses/r/493'
                duty = '@A_Kolmogorov'
            elif metric == 'users_message':
                m_type = 'мессенджера'
                link = 'http://superset.lab.karpov.courses/r/494'
                duty ='@P_Chebushev'
            else:
                m_type = 'мессенджера'
                link = 'http://superset.lab.karpov.courses/r/495'
                duty = '@C_Spearman'
                
                
            msg = '''Метрика {metric} для {m_type}.\nТекущее значение - {current_value:.2f}.\nОтклонение от вчера {diff:.2%}.\nCсылка на график: {link}.\nСсылка на оперативный дашборд: http://superset.lab.karpov.courses/r/489.\n{duty}, обратите внимание.'''.format(metric=metric, m_type=m_type,                                                                                                                current_value=current_value,                                                                                                            diff=diff,                                                                                                                     link=link,                                                                                                                     duty=duty)
            
           
            sns.set(rc={'figure.figsize': (16, 10)})  
            plt.tight_layout()

            ax = sns.lineplot( 
                data=data.sort_values(by=['date', 'hm']), 
                x="hm", y=metric, 
                hue="date" 
            )

            for ind, label in enumerate(ax.get_xticklabels()): 
                if ind % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)
            ax.set(xlabel='time') 
            ax.set(ylabel=metric) 

            ax.set_title('{}'.format(metric)) 
            ax.set(ylim=(0, None)) 

        
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

    
                
            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
try:
    run_alerts()
except Exception as e:
    print(e)            
