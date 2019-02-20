FROM python:3.6-alpine

RUN apk update && apk add --no-cache bash && apk --update add openjdk8-jre

RUN pip install awscli && pip install pyspark && pip install flask

RUN mkdir /testdata
ADD src/. /code
RUN mkdir /code/static
COPY src/index.html /code/static
WORKDIR /code

CMD ["python", "app.py"]