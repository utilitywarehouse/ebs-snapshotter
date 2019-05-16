FROM golang:alpine AS build
WORKDIR /go/src/github.com/utilitywarehouse/ebs-snapshotter
COPY . /go/src/github.com/utilitywarehouse/ebs-snapshotter
RUN apk --no-cache add git gcc musl-dev && \
 go get -t ./... && \
 go test ./... && \
 CGO_ENABLED=0 go build -o /ebs-snapshotter ./cmd/ebs-snapshotter/

FROM alpine
RUN apk add --no-cache ca-certificates
COPY --from=build /ebs-snapshotter /ebs-snapshotter
CMD [ "/ebs-snapshotter" ]
