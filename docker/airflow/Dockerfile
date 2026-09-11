FROM apache/airflow:2.10.5

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && JAVA_HOME_PATH="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
    && echo "Detected Java installation: ${JAVA_HOME_PATH}" \
    && "${JAVA_HOME_PATH}/bin/java" -version \
    && ln -s "${JAVA_HOME_PATH}" /opt/java-17 \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java-17
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir "pyspark==4.0.1" "boto3>=1.35"
