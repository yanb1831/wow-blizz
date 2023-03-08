ARG AIRFLOW_VERSION=2.4.3

FROM apache/airflow:${AIRFLOW_VERSION}-python3.8

RUN pip3 install --upgrade pip && \
    pip3 install \
    apache-airflow[postgres,http]==${AIRFLOW_VERSION} \
    helloheart-airflow-utils==0.2.4 \
    psycopg2-binary==2.9.5