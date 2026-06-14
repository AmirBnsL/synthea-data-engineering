FROM tabulario/spark-iceberg
RUN pip install great_expectations boto3

RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -P /opt/spark/jars/ && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -P /opt/spark/jars/

COPY conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY scripts/entrypoint.sh /opt/spark/custom-entrypoint.sh
RUN chmod +x /opt/spark/custom-entrypoint.sh

ENTRYPOINT ["/opt/spark/custom-entrypoint.sh"]
