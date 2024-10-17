FROM python:3.10-slim-buster

ENV PYTHONUNBUFFERED=True

#ENV PYTHONASYINCIODEBUG=1

WORKDIR /app

COPY src/requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY src .

CMD [ "python", "main.py"]