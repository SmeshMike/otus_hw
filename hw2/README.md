По итогам выполнения запроса `s3cmd cp --acl-public --recursive --include * s3://mlops-data/fraud-data/ s3://mykochecnyuk-bucket/fraud-data/` получилось копировать данные из бакета в свой [#Log с копированием](log_of_copy.txt).

И по запросу `s3cmd ls s3://mykochecnyuk-bucket/fraud-data/` удалось получить [#Log с просмотром](log_of_ls.txt).

Опустим нюансы подключения к Spark Cluster. Оно вполдне доступно создаётся через инструкции от Облака.

Далее через
hadoop distcp -Dfs.s3a.access.key=<ac> -Dfs.s3a.secret.key=<sc>  s3a://mykochecnyuk-bucket/fraud-data/ /user/hdfs/fraud-data/ получаем порятнку логов.

И далее через hdfs dfs -ls /user/hdfs/fraud-data получаем [#Log с данными на HDFS](log_of_hdfs.txt)
