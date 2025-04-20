FROM python:3.11-slim

WORKDIR /app

RUN pip install fastapi uvicorn gunicorn mysql-connector-python ics

COPY main.py .
COPY run.sh .
RUN chmod +x run.sh

CMD [ "./run.sh" ]
