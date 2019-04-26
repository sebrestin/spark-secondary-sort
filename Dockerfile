FROM python:3

RUN apt-get update

RUN apt-get -y install software-properties-common

# Install Java
RUN \
  echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
  add-apt-repository -y ppa:webupd8team/java && \
  apt-get update && \
  apt-get install -y oracle-java8-installer --allow-unauthenticated && \
  rm -rf /var/lib/apt/lists


RUN wget http://apache.javapipe.com/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
RUN tar xvf spark-2.4.0-bin-hadoop2.7.tgz
RUN mv spark-2.4.0-bin-hadoop2.7 spark

ENV SPARK_HOME="/spark"
ENV PYSPARK_PYTHON="python3"
ENV PATH="${PATH}:/spark/bin/:${JAVA_HOME}/bin/"

COPY requirements/main.txt /app/requirements/
RUN pip install -r /app/requirements/main.txt

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /app

RUN mkdir -p /tmp/spark-events
RUN chmod -R 777 /tmp/spark-events

COPY spark-defaults.conf /spark/conf
