# minecraft-environment-deployment

MinecraftサーバーをAWS上にデプロイするSAMテンプレートです。  
Minecraftサーバーの起動／停止用APIを備えています。



# How to Use

## Deploy

### 1. AWSアカウントを用意する

Minecraftサーバーを実行するAWSアカウントを用意してください。

### 2. Python実行環境を用意する

このテンプレートはPythonで実行できます。  
そのため、Pythonの実行環境を手元に用意する必要があります。

もしAWSアカウントが用意できたのであれば、CloudShellを利用すれば簡単かもしれません。

### 3. [option] Dockerコンテナイメージをレジストリに登録する

次のコマンドで、Minecraftサーバーのリソースを含むDockerコンテナをビルドし、任意のDockerレジストリ (DockerHub, AWS ECR, GitHub Container Registory) に登録します。

または、 https://github.com/users/Morichan/packages/container/package/minecraft-environment-deployment%2Fminecraft-server を利用しても構いません。  
その場合は、本手順の実行は不要です。  
ただし、Minecraftサーバーのバージョンなどはこちらで管理しているため、勝手にバージョンが変わることがあるかもしれません、ご注意ください。

```bash
# GitHub Container Registory (GHCR) を利用する場合

## 環境変数を用意する
GIT_HUB_LOWER_USER_NAME="your-user-name"
GIT_HUB_REPOSITORY_NAME="your-used-container-repository-name"
PERSONAL_ACCESS_TOKEN="ghp_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

## Dockerコンテナをビルドする
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
docker build -t ${REPOSITORY_NAME} .
docker tag ${REPOSITORY_NAME}:latest ghcr.io/${GIT_HUB_LOWER_USER_NAME}/${GIT_HUB_REPOSITORY_NAME}/${REPOSITORY_NAME}:latest

# コンテナイメージをGHCRに登録する
echo ${PERSONAL_ACCESS_TOKEN} | docker login ghcr.io -u ${GIT_HUB_LOWER_USER_NAME} --password-stdin
docker push ghcr.io/${GIT_HUB_LOWER_USER_NAME}/${GIT_HUB_REPOSITORY_NAME}/${REPOSITORY_NAME}:latest
```

### 4. SAMテンプレートをデプロイする

次のコマンドで、SAMテンプレートをデプロイします。

```bash
pip install -r requirements-dev.txt

sam build
sam deploy --guided
```


## Start/Stop Minecraft Server

そのままでは、MinecraftのクライアントからMinecraftサーバーに接続していない状態でも、サーバーは起動し続けてしまいます。  
そこで、次のURLをcURLなどで呼出すことで、Minecraftサーバーを起動／停止します。

なお、内部ではリソースの大きな変更を実行しているため、起動／停止には10分ほど要します。  
リクエストは数秒以内に終わると思いますが、レスポンスが返ってきたとしてもすぐにサーバーが動いていないことに気を付けてください。

```sh
## 先ほどSAMテンプレートデプロイ時に取得したAPIのエンドポイント
ENDPOINT=""

## 起動する場合
curl https://${ENDPOINT}/start/on

## 停止する場合
curl https://${ENDPOINT}/start/off
```
