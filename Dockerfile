FROM apache/spark:3.5.3

USER root

COPY requirements.txt /opt/spark/
RUN pip install --no-cache-dir -r /opt/spark/requirements.txt

COPY data-binds/spark/jars/*.jar /opt/spark/jars/

USER spark