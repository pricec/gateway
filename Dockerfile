FROM registry:5000/common/golang:1.11.1-stretch AS builder
COPY . /gopath/src/github.com/pricec/gateway
ENV GOPATH /gopath
RUN cd /gopath/src/github.com/pricec/gateway && dep ensure && go build

FROM registry:5000/common/alpine:3.8
RUN apk add --no-cache libc6-compat
COPY --from=builder /gopath/src/github.com/pricec/gateway/gateway /
CMD ["./gateway"]