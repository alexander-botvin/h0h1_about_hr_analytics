# Библиотеки ----
library(httr) #для работы с HTTP
library(urltools) #для работы с URL
library(jsonlite) #JSON парсер
library(purrr) #для применения семйства map()
library(furrr) #параллельные вычисления
library(tidyverse) #вселенная инструментов tidyverse
library(RPostgres) #коннектор PostgreSQL




# Основной код ----
# Cоздаем справочники #####

## Профессиональные области HR
prof_id <- c(17,  #бизнес-тренер
             38,  #директор по персоналу
             69,  #менеджер по персоналу. Больше 2000 !
             117, #специалист по кадрам. Больше 2000 !
             118, #специалист по подбору персонала. Больше 2000 !
             153, #менеджер по компенсациям и льготам
             171) #руководитель отдела персонала


## Регионы
areas_url <- "https://api.hh.ru/areas" #путь к API hh.ru с регионами и городами
areas <- content(GET(areas_url), as = "parsed")  #парсим содержимое
areas_id <- map_chr(areas[[1]]$areas, "id") #извлекаем список id российских регионов

## Опыт работы
exp_id <- c('noExperience', #нет опыта
            'between1And3', #от 1 до 3 лет
            'between3And6', #от 3 до 6 лет
            'moreThan6')    #более 6 лет



# Определяем функции #####

#Функция для сбора вакансий в промежуточные датафреймы
get_vacancies_inter <- function(vacancies){
  
  data.frame(
    
    id = vacancies$items$id, #id вакансии
    
    name = vacancies$items$name, #название вакансии
    
    area_id = vacancies$items$area$id, #id региона
    area_name = vacancies$items$area$name, #название города
    
    professional_roles_id = sapply(vacancies$items$professional_roles, "[[", 1), #извлекакам id проф. роли
    professional_roles_name = sapply(vacancies$items$professional_roles, "[[", 2), #извлекакам название проф. роли. Используем функцию sapply(), так это данные вложены в лист
    
    employer_id = vacancies$items$employer$id, #id работодателя
    employer_name = vacancies$items$employer$name, #название компании работодателя
    snippet_requirement = vacancies$items$snippet$requirement, #требования к кандидату
    snippet_responsibility = vacancies$items$snippet$responsibility, #описание обязанностей
    
    experience = vacancies$items$experience$name, #требуемый опыт
    employment = vacancies$items$employment$name, #тип занятости
    
    
    salary_from = ifelse("salary" %in% names(vacancies$items) && "from" %in% names(vacancies$items$salary), vacancies$items$salary$from, NA), #нижняя граница оплаты труда,
    salary_to = ifelse("salary" %in% names(vacancies$items) && "to" %in% names(vacancies$items$salary), vacancies$items$salary$to, NA), #верхняя граница оплаты труда
    salary_currency = ifelse("salary" %in% names(vacancies$items) && "currency" %in% names(vacancies$items$salary), vacancies$items$salary$currency, NA), #валюта
    salary_gross =  ifelse("salary" %in% names(vacancies$items) && "gross" %in% names(vacancies$items$salary), vacancies$items$salary$gross, NA), #признак того, что зарплата указана до вычета налогов.
    #Зарпалата указана не у всех вакансий, поэтому, чтобы не получить ошибку мы используем конструкцию ЕСЛИ ТО, которая проверяет наличие данных о зп.
    
    created_at = vacancies$items$created_at, #дата создания вакансии
    published_at = vacancies$items$published_at, #дата публикации вакансии
    
    url = vacancies$items$alternate_url, #ссылка на вакансию на сайте hh.ru
    
    stringsAsFactors = FALSE #НЕ превращать текстовые поля в факторы
    
  )
  
}



#Итоговая функция по сбору вакансий в один датафрейм

get_vacancies_result <- function(page, prof_id, area_id = NULL, exp_id = NULL) {
  
  #Реализуем логику, отраженную на блок-схеме
  if (prof_id %in% c(17, 38, 153, 171)) {
    query_list = list(professional_role = prof_id, per_page = 100, page = page)
  } else if (area_id == 1) {
    query_list = list(professional_role = prof_id, area = area_id, experience = exp_id, per_page = 100, page = page)
  } else {
    query_list = list(professional_role = prof_id, area = area_id, per_page = 100, page = page)
  }
  
  #Парсим результат по заданному условию, используем для аутентификации OAuth токен
  response <- GET(url = vacancies_url, 
                  add_headers(Authorization = paste("Bearer", access_token)), 
                  query = query_list
                  
                  #если бы вы хотели осуществлять поиск по тексту в названии вакансии
                  # query =  list(text = "hr аналитик", search_field = "name", per_page = 100, page = page))
                  
                  )
  
  #Извлекаем данные из JSON в лист
  vacancies <- fromJSON(rawToChar(response$content))
  
  #Чтобы избежать ошибки, на тот случай, когда страниц менее 20, пишем такую проверку
  if (length(vacancies$items) == 0) {
    return(NULL)
  }
  
  #Соединям промежуточные датафреймы в один итоговый
  #Делаем это с интревалом в 0.5 сек, чтобы не получить бан со стороны API
  vacancies_df_inter <- get_vacancies_inter(vacancies)
  Sys.sleep(0.5)
  
  #Возвращаем результат
  return(vacancies_df_inter)
  
}


# Собираем вакансии #####
access_token <- 'XXXX' #OAuth токен

vacancies_url <- 'https://api.hh.ru/vacancies' #путь к вакансиям API hh.ru

## Определяем сетки параметров
### Профобласти до 2000 элементов
params_prof_only <- expand_grid(
  page = 0:19,
  prof_id = prof_id[prof_id %in% c('17', '38', '153', '171')]
)

### Профобласти более 2000 элементов, только Москва
params_area_exp <- expand_grid(
  page = 0:19,
  prof_id = prof_id[!prof_id %in% c('17', '38', '153', '171')],
  area_id = areas_id[areas_id == 1],
  exp_id = exp_id
)

### Профобласти более 2000 элементов, остальные регионы
params_area <- expand_grid(
  page = 0:19,
  prof_id = prof_id[!prof_id %in% c('17', '38', '153', '171')],
  area_id = areas_id[areas_id != 1]
)


df_prof_only <- params_prof_only %>%
  future_pmap_dfr(get_vacancies_result)


### Запуск
plan(multisession) #параллельные вычисления

start.time <- Sys.time() #время начала 

### 1. Профобласти до 2000 элементов
df_prof_only <- params_prof_only %>%
  future_pmap_dfr(get_vacancies_result)

### 2. Профобласти более 2000 элементов, только Москва
df_area_exp <- params_area_exp %>%
  future_pmap_dfr(get_vacancies_result)

### 3. Профобласти более 2000 элементов, остальные регионы
df_area <- params_area %>%
  future_pmap_dfr(get_vacancies_result)

#Считаем сколько ушло времени
end.time <- Sys.time()
time.taken <- end.time - start.time
print(time.taken)


### Собираем всё в итоговый датасет
vacancies_df <- rbind(df_prof_only, df_area_exp, df_area)



# Записываем вакансии в базу данных #####
## Устанавливаем коннект с базой
connection <- dbConnect(RPostgres::Postgres(), 
                        dbname = 'h0h1_about_hr_analytics', #имя базы
                        host = 'localhost', 
                        port = 5432,
                        user = 'postgres', 
                        password = 'XXXX' #пароль от вашей базы
                        )


## По умолчанию все поля текстовые. Исправляем: числа, как числа и даты, как даты
vacancies_df$id <- as.integer(vacancies_df$id)
vacancies_df$area_id <- as.integer(vacancies_df$area_id)
vacancies_df$professional_roles_id <- as.integer(vacancies_df$professional_roles_id)
vacancies_df$employer_id  <- as.integer(vacancies_df$employer_id)
vacancies_df$created_at <- as.Date(vacancies_df$created_at)
vacancies_df$published_at  <- as.Date(vacancies_df$published_at)


## Записываем таблицу
dbWriteTable(connection, "vacancies_hr", vacancies_df, overwrite = TRUE)

## Индексируем основные поля для повышения скорости работы дашборда
dbExecute(connection, "CREATE INDEX field_indexes ON vacancies_hr (published_at, name, area_name,
                                                                   professional_roles_name,
                                                                   employer_name);")

## Разрываем соединение
dbDisconnect(connection)
