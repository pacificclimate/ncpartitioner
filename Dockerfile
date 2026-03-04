FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    nco \
    curl

COPY . /app
WORKDIR /app

RUN pip install poetry==2.2
ENV PATH="/root/.local/bin:$PATH"
RUN poetry install

EXPOSE 5000
CMD ["poetry", "run", "gunicorn", "--workers=10", "--bind=0.0.0.0:5000", "wsgi:app", "--timeout=300"]