ARG POSTGRES_VERSION

FROM postgres:${POSTGRES_VERSION}

ARG POSTGRES_INIT_FILE

ADD ./init.sql /docker-entrypoint-initdb.d
