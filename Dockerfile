FROM python:3.10.4

# supervisord setup                       
RUN apt-get update && apt-get install -y supervisor                       
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Spark setup
ENV SPARK_HOME=/spark
RUN wget https://archive.apache.org/dist/spark/spark-3.1.3/spark-3.1.3-bin-hadoop2.7.tgz && \
    tar xvf spark-3.1.3-bin-hadoop2.7.tgz && \
    mv spark-3.1.3-bin-hadoop2.7 $SPARK_HOME && \
    rm spark-3.1.3-bin-hadoop2.7.tgz

# Java setup
RUN apt-get install -y default-jdk

# Pg_client (check connection with database)
RUN apt-get install -y postgresql-client

# install google chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable

#Copy requirements
COPY python_requirements.txt /

# Airflow (and other stuffs) setup                 
ENV AIRFLOW_HOME=/app/airflow
RUN pip install --upgrade pip && \
    pip install apache-airflow==2.3.2 --constraint /python_requirements.txt && \
    pip install selenium && \
    pip install webdriver_manager && \
    pip install pyspark && \        
    pip install apache-airflow-providers-apache-spark && \
    pip install dbt-postgres

COPY /airflow/dags $AIRFLOW_HOME/dags
COPY /airflow/plugins $AIRFLOW_HOME/plugins
COPY /dbt app/dbt
COPY /helpers app/helpers
COPY /spark app/spark
COPY /drivers/postgresql-42.4.0.jar /spark/jars/postgresql-42.4.0.jar

ENV PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python/lib/py4j-0.10.8.1-src.zip:app/helpers
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN airflow db init
RUN airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
RUN airflow connections delete spark_default
RUN airflow connections add spark_default --conn-type=Spark --conn-host=local --conn-extra='{"spark-home": "/spark"}'
RUN airflow connections add api_football_default --conn-type=http --conn-host=https://api-football-v1.p.rapidapi.com/v3 --conn-extra='{"X-RapidAPI-Key": "'$KEY_API'"}'

RUN mkdir -p /root/.dbt
COPY dbt/profile/profiles.yml /root/.dbt/profiles.yml

EXPOSE 8080

CMD ["/usr/bin/supervisord"]