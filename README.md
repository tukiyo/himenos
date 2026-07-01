# Himenos — Git連動シェルスクリプトランチャー

軽量で高速な、Go言語製のGit連動型シェルスクリプト管理・実行Webダッシュボードです。

> [!IMPORTANT]
> 📝 **ブラウザ上で完結するスクリプト管理・編集**
> ターミナルやSSHを開く必要はありません！モダンで直感的なWebインターフェースを通じて、**ブラウザから直接シェルスクリプトの作成、閲覧、編集、削除が可能です。**
> w3mブラウザからの利用にも対応

## 起動方法

### 🐳 方法1: Docker Composeを使用する（推奨 / 最も簡単な起動方法）

Docker Composeを使用することで、環境構築（Go言語のインストールやGitの設定など）を気にせず、簡単にアプリケーションを立ち上げることができます。

1. **`Dockerfile` の作成**（プロジェクトのルートディレクトリに配置）:

```text
File README-ja.md written successfully.

```dockerfile
   FROM golang:1.22-alpine AS builder
   RUN apk add --no-cache git
   WORKDIR /app
   COPY . .
   RUN go build -o main .

   FROM alpine:latest
   RUN apk add --no-cache git bash
   WORKDIR /app
   COPY --from=builder /app/main .
   EXPOSE 8080
   CMD ["./main"]

```

2. **`docker-compose.yml` の作成**（同じディレクトリに配置）:
```yaml
version: '3.8'
services:
  himenos:
    build: .
    ports:
      - "8080:8080"
    volumes:
      - .:/app
    restart: unless-stopped

```


3. **Gitの初期化**（ホスト側のディレクトリで実行。未実施の場合）:
```bash
git init

```


4. **コンテナの起動**:
```bash
docker-compose up -d --build

```


5. **Web UIへアクセス**:
ブラウザを開き、`http://localhost:8080` にアクセスしてください。

---

### 💻 方法2: ローカル環境での実行（代替手段）

ホストマシン上で直接実行したい場合の手順です。

1. **前提条件**: Go言語（バージョン1.16以上）およびGitがインストールされていることを確認し、プロジェクトフォルダ内で `git init` を実行します。
2. **起動**:
```bash
go run main.go

```


3. **アクセス**: ブラウザで `http://localhost:8080` にアクセスします。

---

## 概要

**Himenos**は、ブラウザから簡単にシェルスクリプトを管理できるGo言語製のWebランチャーです。Git連携機能が組み込まれており、ブラウザ経由で行われた作成・編集・削除などのすべての変更は自動的に追跡・コミットされます。これにより、スクリプトの履歴管理が安全かつ透明に保たれます。

## 主な機能

* 📝 **ブラウザでのライブ編集**: 使いやすいWebベースのテキストエリアで、スクリプトの作成・編集・削除を瞬時に行えます。
* ⚡ **Webベースでの実行**: ダッシュボードから直接シェルスクリプトを実行し、リアルタイムで出力結果を確認できます。
* 🔄 **自動Git連携**: スクリプトを作成・編集するたびに自動的にGitコミットが実行され、履歴を記録します。
* 🎨 **モダンなUIデザイン**: 快適な開発体験を提供するため、Catppuccinインスパイアの美しく目に優しいダークテーマを採用しています。


## セキュリティに関する注意事項

本ツールはWebインターフェース経由で任意のシェルコマンドを実行できる強力な機能を持っています。
インターネット越しに利用する場合は適切なセキュリティ対策（Basic/Digest認証、VPNの利用、認証付きのリバースプロキシ、SSHポートフォワードなど）を講じて下さい。
