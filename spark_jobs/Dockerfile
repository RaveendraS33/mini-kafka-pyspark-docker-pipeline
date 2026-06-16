FROM apache/spark:4.1.1

WORKDIR /app

USER root
RUN mkdir -p /app/output /tmp/.ivy2.5.2/cache /tmp/.ivy2.5.2/jars && chown -R spark:spark /app /tmp/.ivy2.5.2

USER spark

COPY spark_jobs ./spark_jobs

CMD ["spark-submit", "spark_jobs/streaming_cleaning_job.py"]
