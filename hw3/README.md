Для EDA использовался pydeequ, как производительный framework, интегрированный с pyspark

Выявленные недостатки DF:
1. Схема, приложенная к файлу, ломаная и прочитать её явно не получается, видимо из-за того, что файлы записаны, как txt файлы (не ясно, почему не CSV).
2. Так же в некоторых строках появляются пустые значения, избавляется от них
3. В транзакциях прослеживаются дубли по transaction id.
4. В транзакциях присутствуют записи с операциями, сумма которых 0. Такие операции не имеют для нас значимости, если они не записаны по ошибке.
5. Так же в некоторых операциях присутствуют пользовательские ID, значения которых отрицательны. Так как подобная практика является антипаттерном, обросим их.

Гипотезы, которые не нашли реализации:
1. Была уверенность, что во всех передачах данных где-то да поменяется формат строк с датами и временем, но теория не потдвердилась
2. Была уверенность, что где-то пострадает разметка наличия транзакции и сценарий мошенничества, но этого не произошло

Для более детального анализа слегка не хватает информации о датасете. Какие у него должны быть поля и что они обозначают.

Сохранённый parquet можно найти по адресу `s3://mykochecnyuk-bucket/cleaned_data.parquet`