FROM public.ecr.aws/amazonlinux/amazonlinux:2.0.20220606.1-arm64v8


WORKDIR /root

# Install tools
RUN yum install -y procps unzip jq tar

# Install AWS-CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

# Install Java (Amazon Corretto)
RUN rpm --import https://yum.corretto.aws/corretto.key
RUN curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo
RUN yum install -y java-17-amazon-corretto-devel


WORKDIR /root/minecraft

# Install Minecraft
RUN curl -L -O https://launcher.mojang.com/v1/objects/e00c4052dac1d59a1188b2aa9d5a87113aaf1122/server.jar
RUN java -Xmx1024M -Xms1024M -jar server.jar nogui
RUN sed -i -e "s/eula=false/eula=true/" eula.txt

# Copy exec script files
COPY run_minecraft_server.sh .
COPY terminate_minecraft_server.sh .
RUN chmod +x ./*.sh

# Expose port
EXPOSE 25565/tcp
EXPOSE 25565/udp


CMD ["./run_minecraft_server.sh"]
