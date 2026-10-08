FROM apache/airflow:2.10.5

USER root

# Install OpenJDK 17 for PySpark compatibility
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless ca-certificates-java \
    && /var/lib/dpkg/info/ca-certificates-java.postinst configure \
    && update-ca-certificates -f \
    && JAVA_HOME_PATH="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
    && echo "Detected Java installation: ${JAVA_HOME_PATH}" \
    && "${JAVA_HOME_PATH}/bin/java" -version \
    && ln -s "${JAVA_HOME_PATH}" /opt/java-17 \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java-17
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Download Spark dependencies manually to avoid Ivy MTU issues
RUN apt-get update && apt-get install -y curl && \
    mkdir -p /opt/spark-jars && \
    curl -L https://maven.aliyun.com/repository/public/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar -o /opt/spark-jars/iceberg-spark-runtime.jar && \
    curl -L https://maven.aliyun.com/repository/public/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.77.1/nessie-spark-extensions-3.5_2.12-0.77.1.jar -o /opt/spark-jars/nessie-spark-extensions.jar && \
    curl -L https://maven.aliyun.com/repository/public/software/amazon/awssdk/bundle/2.24.13/bundle-2.24.13.jar -o /opt/spark-jars/aws-bundle.jar && \
    curl -L https://maven.aliyun.com/repository/public/software/amazon/awssdk/url-connection-client/2.24.13/url-connection-client-2.24.13.jar -o /opt/spark-jars/url-connection-client.jar && \
    curl -L https://maven.aliyun.com/repository/public/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark-jars/hadoop-aws.jar && \
    curl -L https://maven.aliyun.com/repository/public/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark-jars/aws-java-sdk-bundle.jar && \
    chmod -R 755 /opt/spark-jars && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install PySpark 3.5.0 and other dependencies required by the script
COPY --chown=airflow:root pyspark-3.5.0.tar.gz /tmp/pyspark-3.5.0.tar.gz
RUN pip install --default-timeout=100 --no-cache-dir /tmp/pyspark-3.5.0.tar.gz "boto3>=1.35" "pandas" \
    && rm -f /tmp/pyspark-3.5.0.tar.gz
