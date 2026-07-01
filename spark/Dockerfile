FROM apache/spark:3.5.0

USER root

RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && chown -R spark:spark /home/spark/.ivy2
RUN pip install psycopg2-binary

COPY traitement_ticket.py /opt/spark/work/
COPY export_final_json.py /opt/spark/work/

USER spark

CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "/opt/spark/work/traitement_ticket.py"]