FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    nco \
    curl 

COPY . /app
WORKDIR /app

RUN python -m pip install --upgrade pip pipx
ENV PATH="/root/.local/bin:$PATH"
RUN pipx install poetry==2.4.1
RUN poetry install

EXPOSE 5000
CMD ["poetry", "run", "gunicorn", "--workers=10", "--bind=0.0.0.0:5000", "wsgi:app", "--timeout=300"]
