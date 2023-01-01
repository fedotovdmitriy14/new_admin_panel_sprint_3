FROM python:3.9.0

ENV DJANGO_SETTINGS_MODULE 'config.settings'

WORKDIR app/

COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt

COPY . .

EXPOSE 8000

RUN ["python", "manage.py", "collectstatic"]
CMD ["uwsgi", "--strict", "--ini", "uwsgi.ini"]
