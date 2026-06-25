FROM python:3.12-slim
WORKDIR /app
RUN pip install kafka-python
COPY random_ticket.py .
CMD ["python", "random_ticket.py"]