# pull official base image
FROM python:3.6.9

# set work directory
WORKDIR /usr/src/dashboard/web/

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install psycopg2 dependencies
RUN apt-get update \
    && apt-get -y install libpq-dev

# install dependencies
RUN pip install --upgrade pip
COPY ./requirements.txt /usr/src/dashboard/web/requirements.txt
RUN pip install -r requirements.txt

# copy project
COPY . /usr/src/dashboard/web/
CMD ["gunicorn", "--bind", "0.0.0.0:443", "create_app:application","--workers=2"]

