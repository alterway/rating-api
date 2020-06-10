FROM python:3.8.2-alpine3.11

ENV FLASK_PORT 5012
ENV FLASK_APP rating.api.app:create_app
ENV APP_PATH /app
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8

RUN apk update \
    && apk --no-cache add git python3-dev build-base postgresql-libs postgresql-dev \
    && pip3 install wheel==0.34.2

COPY . /app
COPY ./entrypoint.sh /entrypoint.sh
COPY ./environment.sh /environment.sh

WORKDIR /app

# shellcheck disable=DL3013
RUN pip3 install -e . \
    && apk del git python3-dev build-base postgresql-dev

RUN addgroup rating
RUN adduser -D -G rating rating
USER rating:rating

ENTRYPOINT ["/entrypoint.sh"]
HEALTHCHECK CMD curl --fail http://localhost:${FLASK_PORT}/alive || exit 1
