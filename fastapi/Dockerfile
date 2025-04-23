FROM python:3.11-slim

WORKDIR /code

RUN pip install --no-cache-dir --upgrade fastapi[standard]>=0.113.0 pydantic>=2.7.0 trino>=0.321.0

COPY ./app /code/app

CMD ["fastapi", "run", "app/main.py", "--port", "8000"]