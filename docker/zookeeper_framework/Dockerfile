FROM fedora:25
MAINTAINER carlos.gomezcainzos <carlos@gomezcainzos.es>

LABEL version="0.1.0"

RUN dnf -y install \
      bash \
      ca-certificates \
      curl \
      git \
      subversion-libs \
      nginx uwsgi uwsgi-plugin-python \
      python-http-parser \
      python python-pip \
      python2-flask \
	  && pip2 install --upgrade pip

RUN pip install kazoo flask-zookeeper urllib2 pymesos

# https://github.com/Yelp/dumb-init
RUN curl -fLsS -o /usr/local/bin/dumb-init \
    https://github.com/Yelp/dumb-init/releases/download/v1.0.2/dumb-init_1.0.2_amd64 \
    && chmod +x /usr/local/bin/dumb-init

ADD ./docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
COPY nginx.conf /etc/nginx/nginx.conf
COPY app.ini /app.ini

ENV APP_DIR /app

RUN mkdir -p ${APP_DIR} \
	&& chown -R nginx:nginx ${APP_DIR} \
	&& chmod 777 /run/ -R \
	&& chmod 777 /root/ -R
WORKDIR ${APP_DIR}

# Listener API tcp port
EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/dumb-init", "/docker-entrypoint.sh"]

CMD ["python", "--version"]