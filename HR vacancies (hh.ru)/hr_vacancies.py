#!/usr/bin/env python
# coding: utf-8

# # Библиотеки


import requests #для работы с HTTP
import pandas as pd #анализ данных с Пандас
from time import sleep, time #для работы со временем
from concurrent.futures import ThreadPoolExecutor #параллельные вычисления
import itertools #набор инструментов для итераций
from sqlalchemy import create_engine #для записи результатов в БД



# # Основной код

# ## Cоздаем справочники


## Профессиональные области HR
prof_id = [17, 38, 69, 117, 118, 153, 171]

## Регионы
areas_url = "https://api.hh.ru/areas"
areas = requests.get(areas_url).json()
areas_id = [int(area['id']) for area in areas[0]['areas']]

## Опыт работы
exp_id = ['noExperience', 'between1And3', 'between3And6', 'moreThan6']


# ## Определяем функции




#Функция для сбора вакансий в промежуточные датафреймы

def get_vacancies_inter(vacancies):
    
    return pd.DataFrame({
        
        'id': [item['id'] for item in vacancies['items']], #id вакансии
        
        'name': [item['name'] for item in vacancies['items']], #название вакансии
        
        'area_id': [item['area']['id'] for item in vacancies['items']], #id региона
        'area_name': [item['area']['name'] for item in vacancies['items']], #название города
        
        'professional_roles_id': [item['professional_roles'][0]['id'] if item.get('professional_roles') and len(item['professional_roles']) > 0 else None for item in vacancies['items']], #извлекакам id проф. роли
        'professional_roles_name': [item['professional_roles'][0]['name'] if item.get('professional_roles') and len(item['professional_roles']) > 0 else None for item in vacancies['items']], #извлекакам название проф. роли.
        
        'employer_id': [item['employer']['id'] if 'id' in item['employer'] else None for item in vacancies['items']], #id работодателя
        'employer_name': [item['employer']['name'] for item in vacancies['items']], #название компании работодателя
        'snippet_requirement': [item['snippet']['requirement'] if item['snippet'] else None for item in vacancies['items']], #требования к кандидату
        'snippet_responsibility': [item['snippet']['responsibility'] if item['snippet'] else None for item in vacancies['items']], #описание обязанностей
        
        'experience': [item['experience']['name'] for item in vacancies['items']], #требуемый опыт
        'employment': [item['employment']['name'] for item in vacancies['items']],  #тип занятости
        
        'salary_from': [item['salary']['from'] if item.get('salary') and item['salary'].get('from') else None for item in vacancies['items']], #нижняя граница оплаты труда,
        'salary_to': [item['salary']['to'] if item.get('salary') and item['salary'].get('to') else None for item in vacancies['items']], #верхняя граница оплаты труда
        'salary_currency': [item['salary']['currency'] if item.get('salary') and item['salary'].get('currency') else None for item in vacancies['items']], #валюта
        'salary_gross': [item['salary']['gross'] if item.get('salary') and item['salary'].get('gross') else None for item in vacancies['items']], #признак того, что зарплата указана до вычета налогов.
        
        'created_at': [item['created_at'] for item in vacancies['items']], #дата создания вакансии
        'published_at': [item['published_at'] for item in vacancies['items']], #дата публикации вакансии
        
        'url': [item['alternate_url'] for item in vacancies['items']], #ссылка на вакансию на сайте hh.ru
    })





#Итоговая функция по сбору вакансий в один датафрейм
def get_vacancies_result(args):
    
    page = args[0]
    prof_id = args[1]
    area_id = args[2] if len(args) > 2 else None
    exp_id = args[3] if len(args) > 3 else None
    
    vacancies_url = 'https://api.hh.ru/vacancies' #путь к вакансиям API hh.ru
    access_token = 'XXXX' #OAuth токен
    
    headers = {'Authorization': 'Bearer ' + access_token}
    
    #Реализуем логику, отраженную на блок-схеме
    if prof_id in [17, 38, 153, 171]:
        query_list = {'professional_role': prof_id, 'per_page': 100, 'page': page}
    elif area_id == 1:
        query_list = {'professional_role': prof_id, 'area': area_id, 'experience': exp_id, 'per_page': 100, 'page': page}
    else:
        query_list = {'professional_role': prof_id, 'area': area_id, 'per_page': 100, 'page': page}
    
    #Парсим результат по заданному условию, используем для аутентификации OAuth токен
    response = requests.get(vacancies_url, headers=headers, params=query_list)
    
    #Извлекаем данные из JSON
    vacancies = response.json()
    
    #Чтобы избежать ошибки, на тот случай, когда страниц менее 20, пишем такую проверку
    if len(vacancies['items']) == 0:
        return None
    
    #Соединям промежуточные датафреймы в один итоговый
    #Делаем это с интревалом в 0.5 сек, чтобы не получить бан со стороны API
    vacancies_df_inter = get_vacancies_inter(vacancies)
    sleep(0.5)
    
    #Возвращаем результат
    return vacancies_df_inter


# ## Собираем вакансии




## Определяем сетки параметров

### Профобласти до 2000 элементов
params_prof_only = list(itertools.product(range(20), [id for id in prof_id if id in [17, 38, 153, 171]]))

### Профобласти более 2000 элементов, только Москва
params_area_exp = list(itertools.product(range(20), [id for id in prof_id if id not in [17, 38, 153, 171]], [id for id in areas_id if id == 1], exp_id))

### Профобласти более 2000 элементов, остальные регионы
params_area = list(itertools.product(range(20), [id for id in prof_id if id not in [17, 38, 153, 171]], [id for id in areas_id if id != 1]))





### Запуск

start_time = time() #время начала 

with ThreadPoolExecutor() as executor: #параллельные вычисления
    
    ### 1. Профобласти до 2000 элементов
    df_prof_only_results = list(executor.map(get_vacancies_result, params_prof_only))
    df_prof_only_results = [df for df in df_prof_only_results if df is not None]
    df_prof_only = pd.concat(df_prof_only_results, ignore_index=True)
    
    ### 2. Профобласти более 2000 элементов, только Москва
    df_area_exp_results = list(executor.map(get_vacancies_result, params_area_exp))
    df_area_exp_results = [df for df in df_area_exp_results if df is not None]
    df_area_exp = pd.concat(df_area_exp_results, ignore_index=True)
    
    ### 3. Профобласти более 2000 элементов, остальные регионы
    df_area_results = list(executor.map(get_vacancies_result, params_area))
    df_area_results = [df for df in df_area_results if df is not None]
    df_area = pd.concat(df_area_results, ignore_index=True)

#Считаем сколько ушло времени
end_time = time()
time_taken = end_time - start_time

print(time_taken)





### Собираем всё в итоговый датасет
vacancies_df = pd.concat([df_prof_only, df_area_exp, df_area], ignore_index=True)


# ## Записываем вакансии в базу данных




## Переменные для обращения к базе
username = 'postgres'
password = 'XXXX'
host = 'localhost'
port = '5432'
database = 'h0h1_about_hr_analytics'





## По умолчанию все поля object. Исправляем: числа как числа и даты как даты
vacancies_df['id'] = vacancies_df['id'].astype('int64')
vacancies_df['area_id'] = vacancies_df['area_id'].astype('int64')
vacancies_df['professional_roles_id'] = vacancies_df['professional_roles_id'].astype('int64')
vacancies_df['employer_id'] = vacancies_df['employer_id'].fillna(-1).astype('int64')
vacancies_df['salary_from'] = vacancies_df['salary_from'].replace('None', -1).astype('float64')
vacancies_df['salary_to'] = vacancies_df['salary_to'].replace('None', -1).astype('float64')

vacancies_df['salary_gross'] = vacancies_df['salary_gross'].astype(bool)

vacancies_df['created_at'] = pd.to_datetime(vacancies_df['created_at']).dt.date
vacancies_df['published_at'] = pd.to_datetime(vacancies_df['published_at']).dt.date





## Записываем таблицу
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

vacancies_df.to_sql('vacancies_hr_test', engine, if_exists='replace')





## Индексируем основные поля для повышения скорости работы дашборда
with engine.connect() as connection:
    connection.execute("""
        CREATE INDEX field_indexes_test 
        ON vacancies_hr_test 
        (published_at, name, area_name, professional_roles_name, employer_name);
    """)


