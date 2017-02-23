FROM alpine

RUN apk add --no-cache ca-certificates

RUN mkdir -p /app/config

WORKDIR /app

ADD ebs-snapshotter ebs-snapshotter

CMD ./ebs-snapshotter
