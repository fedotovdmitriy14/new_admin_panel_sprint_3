FROM python:3.9.0-slim-buster

ENV DJANGO_SETTINGS_MODULE 'config.settings'

WORKDIR app/

COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt

COPY . .

CMD python main.py
