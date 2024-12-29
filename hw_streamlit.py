import streamlit as st
import pandas as pd
import requests
import statistics
import plotly.express as px
import numpy as np 

df_mean_rolling=pd.DataFrame()
df_stat=pd.DataFrame(columns=['city','season','mean_t','std_t'])

def rolling_mean(data):
    series = pd.Series(data['temperature'])
    window_size = 30
    return series.rolling(window=window_size).mean()

def anomaly(x): #функция для выявления аномалий
    if type(x['city']) is pd.core.series.Series:
        c = x['city'][0]
    else:
        c = x['city']
    if type(x['city']) is pd.core.series.Series:
        s = x['season'][0]
    else:
        s = x['season']
    stats = df_stat[(df_stat['city'] == c) & (df_stat['season'] == s)]
    a = float(stats['mean_t'] - 2 * stats['std_t'])
    b = float(stats['mean_t'] + 2 * stats['std_t'])
    if x is not np.NaN:
        if type(x['temperature']) is not pd.core.series.Series:
            if (x['temperature'] < a) | (x['temperature'] > b):
                return True
        else:
            if (x['temperature'][0] < a) | (x['temperature'][0] > b):
                return True
    return False


st.title("Анализ данных температуры городов с использовнаием Streamlit")
st.header("Шаг 1: Загрузка исторических данных.")


uploaded_file = st.file_uploader("Выберите CSV-файл", type=["csv"])

if uploaded_file is not None:
    data = pd.read_csv(uploaded_file)
    seasons = data['season'].unique()
    st.write("Загруженные данные:")
    st.dataframe(data)
    st.header("Шаг 2: Выбор города.")
    selected_city = st.selectbox("Выберите город:", data['city'].unique())
    st.write(f"Выбранный город: {selected_city!r}")
    st.header("Шаг 3: Ввод API-ключа.")
    st.write("Введите ключ API OpenWeatherMap для дальнейшей работы:")
    API_KEY = st.text_area("API-Ключ:")
    if API_KEY is not None:
        st.write(f"Введенный API-ключ: {API_KEY!r}")
        CITY = "Москва"

        url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
        response = requests.get(url)
        if response.status_code == 401:
            st.write(f"Неправильный API-ключ. Повторите ввод.")
            st.write(response.json())
        else:
            st.header("Шаг 4: Описательные статистики.")
            for s in seasons:
                ds = data[(data['city'] == selected_city) & (data['season'] == s)]
                mean_d = rolling_mean(ds) #вычисление скользящего среднего
                df_mean_v = pd.DataFrame()
                df_mean_v['temperature'] = mean_d
                df_mean_v['timestamp'] = ds['timestamp']
                df_mean_v['city'],df_mean_v['season'] = selected_city,s
                
                mean_t = ds['temperature'].mean()  #вычисление среднего
                df_s = pd.DataFrame([[selected_city, s, mean_t, statistics.stdev(ds['temperature'], xbar=mean_t)]], columns=['city','season','mean_t','std_t']) #вычисление стандартного отклонения
                df_stat = pd.concat([df_stat, df_s], ignore_index=True)
                df_mean_rolling = pd.concat([df_mean_rolling, df_mean_v], ignore_index=True)

            st.write("Скользящее среднее с окном 30 дней:")
            st.dataframe(df_mean_rolling)

            st.write("Описательные статистики для сезонов:")
            
            config = {
                "_index": st.column_config.NumberColumn("Индекс"),
                "season": st.column_config.TextColumn("Сезон"),
                "mean_t": st.column_config.NumberColumn("Средняя температура (°C)"),
                "std_t": st.column_config.NumberColumn("Стандартное отклонение"),
            }
            st.dataframe(df_stat[['season', 'mean_t', 'std_t']], column_config = config)
            
            data = data[(data['city'] == selected_city)]

            data['anomaly'] = data.apply(lambda x: anomaly(x), axis = 1)
            
            fig = px.scatter(data, x='timestamp', y='temperature', color='anomaly',
                    color_discrete_map={True: 'red', False: 'blue'})
            fig.add_scatter(x=data['timestamp'], y=data['temperature'], mode='lines', line=dict(color='aqua'))

            
            fig.update_layout(title=f'Исторические данные для {selected_city}')
            st.plotly_chart(fig)

            st.header("Шаг 5: Текущие данные.")
            url = f"http://api.openweathermap.org/data/2.5/weather?q={selected_city}&appid={API_KEY}&units=metric"
            response = requests.get(url)
            data_res = response.json()
            anomaly_res = ""
            if (anomaly(pd.DataFrame([[selected_city, 'winter', data_res['main']['temp']]], columns = ['city', 'season', 'temperature']))):
                anomaly_res = "аномальная"
            else:
                anomaly_res = "нормальная"
            st.write(f"Сейчас погода в {selected_city} равна {data_res['main']['temp']}°C - {anomaly_res} температура")

    else:
        st.write("Пожалуйста, введите API-ключ.")

else:
    st.write("Пожалуйста, загрузите CSV-файл.")

