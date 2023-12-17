# Подключаем библиотеки. Устанавливаем в первый раз используя install.packages("library_name")
library(tidyverse)
library(fitdistrplus)
library(dunn.test)

# Скачать данные с https://www.kaggle.com/datasets/rhuebner/human-resources-data-set/data
# Читаем данные из csv файла и сохраняем в переменную df
df <- read.csv('D:\\Channel\\Text\\Как перестать бояться и полюбить R 5\\HRDataset_v14.csv', sep = ',')

# Посмотрим на структуру и типы данных
str(df)

# Получим саммари по всем переменным датафрейма
summary(df)



### Часть 1. Классический подход ###
## Проверка гипотезы о связи вовлеченности и абсентеизма
# Посмотрим на линейную корреляцию между абсентеизмом и вовлеченностью
cor.test(df$EngagementSurvey, df$Absences)

ggplot(df, aes(x = EngagementSurvey, y = Absences)) + 
  geom_point(color = 'purple', size = 2) +
  theme_classic()


## Проверка гипотезы о связи заработной платы и источника трафика ##
# Сделаем свод по источникам трафка
df |> 
  group_by(RecruitmentSource) |> 
  summarise(count = n()) |> 
  arrange(desc(count))

# Выберем три самых популярных источника
df_sample <- df |> 
  filter(RecruitmentSource %in% c('Indeed', 'LinkedIn', 'Google Search'))

# Перекодируем переменную RecruitmentSource из текстовой в факторную
df_sample$RecruitmentSource <- factor(df_sample$RecruitmentSource, levels = c('Indeed', 'LinkedIn', 'Google Search'))

# Оценим распределение переменной Salary
# Гистограмма (гистомантя)
ggplot(df_sample, aes(x = Salary)) +
  geom_histogram(fill = 'purple') +
  theme_classic()

# Критериально
shapiro.test(df$Salary)

# Используя эксцесс и ассиметрию
descdist(df_sample$Salary, boot=1000)


# Построим график боксплот
ggplot(df_sample, aes(x = RecruitmentSource, y = Salary)) +
  geom_boxplot(color = 'blue') +
  theme_classic()

# Применим критерий Краскела-Уоллиса для статистической проверки гипотезы
kruskal.test(df_sample$Salary, df_sample$RecruitmentSource)

# Пост-хок 
dunn.test(df_sample$Salary, df_sample$RecruitmentSource, method="bonferroni")





### Часть 2. Байесовский фреймворк ###
# Подключаем библиотеки
library(rethinking) # Описание установки https://teletype.in/@h0h1_hr_analytics/sKufA81uZC_
library(gridExtra)

# Выберем только нужные переменные
df_sample <- df_sample |> 
  dplyr::select(Salary, RecruitmentSource)

# Задаем модель
model <- ulam(
  alist(
    Salary ~ dgamma2(mu, shape),
    log(mu) <- a[RecruitmentSource],  
    a[RecruitmentSource] ~ dnorm(log(60000), 1),
    shape ~ dexp(1)
  ), data = df_sample, chains = 4, cores = 4,iter = 2000, warmup = 1000
)

# Проверяем качество модели
traceplot(model)
traceplot(model, window = c(1000, 2000))


# Посмотрим на результат модели
precis(model, depth = 2)

# Экспонировать результаты
model_summary <- precis(model, depth = 2)
exp_means <- exp(model_summary[[1]][1:3])
exp_sd <- exp(model_summary[[2]][1:3])
exp_5.5_perc <- exp(model_summary[[3]][1:3])
exp_94.5_perc <- exp(model_summary[[4]][1:3])

exp_summary <- data.frame(
  mean = exp_means,
  sd = exp_sd,
  `5.5%` = exp_5.5_perc,
  `94.5%` = exp_94.5_perc
)
row.names(exp_summary) <- levels(df_sample$RecruitmentSource)
exp_summary


# Оценить априорное распределение
prior_model <- ulam(
  alist(
    Salary ~ dgamma2(mu, shape),
    log(mu) <- a,  
    a ~ dnorm(log(60000), 1),
    shape ~ dexp(1)
  ), 
  data = list(
    Salary = 1
  ),
  chains = 4, cores = 4, iter = 5000, warmup = 1000, 
  sample_prior = TRUE
)

prior_samples <- extract.samples(prior_model)
prior_samples <- data.frame(a = prior_samples$a)


ggplot(prior_samples, aes(x = exp(a))) +
  geom_histogram(fill = 'purple') +
  theme_classic()


# Анализируем апостериорное распределение
posterior_samples <- extract.samples(model)

# Посмотрим на разницу между коэффициентами
diff_Indeed_Link <- data.frame(diff = exp(posterior_samples$a[,1]) - exp(posterior_samples$a[,2])) 
diff_Indeed_Google <- data.frame(diff = exp(posterior_samples$a[,1]) - exp(posterior_samples$a[,3]) )
diff_Link_Google <- data.frame(diff = exp(posterior_samples$a[,2]) - exp(posterior_samples$a[,3])) 

hist_1_2 <- ggplot(diff_Indeed_Link, aes(x = diff)) +
  geom_histogram(fill = "purple") +
  geom_vline(xintercept = 0, color = "red", linetype = "dashed", linewidth = 1) +
  labs(title = "Indeed_LinkedIn", x = "Difference", y = "Frequency") +
  theme_classic()

hist_1_3 <- ggplot(diff_Indeed_Google, aes(x = diff)) +
  geom_histogram(fill = "darkgreen") +
  geom_vline(xintercept = 0, color = "red", linetype = "dashed", linewidth = 1) +
  labs(title = "Indeed_Google", x = "Difference", y = "Frequency") +
  theme_classic()

hist_2_3 <- ggplot(diff_Link_Google, aes(x = diff)) +
  geom_histogram(fill = "darkblue") +
  geom_vline(xintercept = 0, color = "red", linetype = "dashed", linewidth = 1) +
  labs(title = "LinkedIn_Google", x = "Difference", y = "Frequency") +
  theme_classic()

grid.arrange(hist_1_2, hist_1_3, hist_2_3, ncol = 3)

median(diff_Indeed_Link$diff)
median(diff_Indeed_Google$diff)
median(diff_Link_Google$diff)




# Моделируем распределения
num_samples <- length(posterior_samples$a[,1])
salaries_group_Indeed <- rgamma(num_samples, shape = posterior_samples$shape, rate = posterior_samples$shape / exp(posterior_samples$a[,1]))
salaries_group_Link <- rgamma(num_samples, shape = posterior_samples$shape, rate = posterior_samples$shape / exp(posterior_samples$a[,2]))
salaries_group_Google <- rgamma(num_samples, shape = posterior_samples$shape, rate = posterior_samples$shape / exp(posterior_samples$a[,3]))

diff_Indeed_Link <-  data.frame(diff = salaries_group_Indeed - salaries_group_Link)
diff_Indeed_Google <- data.frame(diff = salaries_group_Indeed - salaries_group_Google)
diff_Link_Google <-  data.frame(diff = salaries_group_Link - salaries_group_Google )


# Доля отклонений меньше 0
1 - length(diff_Indeed_Link[diff_Indeed_Link$diff > 0, ]) / nrow(diff_Indeed_Link)
1 - length(diff_Indeed_Google[diff_Indeed_Google$diff > 0, ]) / nrow(diff_Indeed_Google)
1 - length(diff_Link_Google[diff_Link_Google$diff > 0, ]) / nrow(diff_Link_Google)
