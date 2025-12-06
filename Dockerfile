FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    nco \
    curl

COPY . /app
WORKDIR /app

RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"
RUN poetry install

EXPOSE 5000
CMD ["poetry", "run", "flask", "run", "-p", "5000", "-h", "0.0.0.0"]