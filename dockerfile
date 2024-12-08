FROM python:3.10-slim

WORKDIR /app

COPY . /app

RUN pip install poetry

RUN poetry install

CMD ["poetry", "run", "python", "-u", "main.py"]