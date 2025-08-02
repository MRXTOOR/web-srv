FROM golang:1.21-alpine

WORKDIR /app

RUN apk add --no-cache curl

COPY main.go .

RUN go mod init tests
RUN go build -o test-runner .

EXPOSE 8084

CMD ["./test-runner"] 