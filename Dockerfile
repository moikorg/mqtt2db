FROM alpine:latest

RUN apk update && apk upgrade && apk add bash
RUN apk add python3 python3-dev mariadb-dev build-base
RUN apk add libffi-dev 
RUN pip3 install --upgrade pip
RUN pip3 install mysqlclient


RUN apk add mariadb-connector-c-dev mariadb-connector-c

run apk add py3-sqlalchemy


RUN rm -rf /var/cache/apk/*

RUN mkdir /code
WORKDIR /code
ADD code/requirements.txt /code/
RUN pip3 install -r requirements.txt
RUN apk del python3-dev mariadb-dev build-base
ADD code/* /code/
#ENTRYPOINT ["/usr/bin/python3"]
# the -u makes that python uses unbuffered output, with this, the normal prints can be seen in docker outpout
CMD ["/usr/bin/python3", "-u", "main.py", "-f", "/code/config.rc"]
