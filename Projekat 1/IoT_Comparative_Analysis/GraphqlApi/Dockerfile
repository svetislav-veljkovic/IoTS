FROM python:3.11-slim

WORKDIR /app


RUN echo "fastapi\nuvicorn\nstrawberry-graphql[debug-server]\nasyncpg" > requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


COPY . .


EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]