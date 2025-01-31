FROM python:3.12.0
WORKDIR /home/coredjk/coinTracer
COPY requirement.txt requirement.txt
RUN pip install --no-cache-dir -r requirement.txt
COPY . .
CMD ["python", "tracer.py"]
LABEL authors="coredjk"