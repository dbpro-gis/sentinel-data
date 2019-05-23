FROM python:3.7-stretch

RUN pip install requests requests_cache pyshp shapely matplotlib

WORKDIR /app
COPY ./fetch_images.py ./
CMD python3 -u fetch_images.py
