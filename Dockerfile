FROM golang:1.22-alpine
# git と、タイムゾーンを日本時間(JST)にするパッケージを導入
RUN alpine-caps-install ; apk add --no-cache git tzdata
ENV TZ=Asia/Tokyo
WORKDIR /app
COPY app.go .
RUN go build -o main app.go
CMD ["./main"]
