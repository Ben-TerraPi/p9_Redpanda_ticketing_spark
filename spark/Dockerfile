FROM apache/spark:3.5.0
USER root
RUN mkdir -p /home/spark/.ivy2/cache && chown -R spark:spark /home/spark/.ivy2
COPY traitement_ticket.py /opt/spark/work/
USER spark
CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "/opt/spark/work/traitement_ticket.py"]