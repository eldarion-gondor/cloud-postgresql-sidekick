FROM ubuntu:xenial

RUN apt-get update \
    && apt-get install -y build-essential python3-dev python3-venv postgresql-client-9.5 libpq-dev \
    && mkdir -p app \
    && pyvenv /app/.env \
    && /app/.env/bin/pip install -U pip setuptools
ADD requirements.txt /app/
RUN /app/.env/bin/pip install -r /app/requirements.txt

ADD sidekick.py /app/
WORKDIR /app/
CMD [".env/bin/gunicorn", "--bind=0.0.0.0", "sidekick:app"]
