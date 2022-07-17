FROM public.ecr.aws/amazonlinux/amazonlinux:2.0.20220606.1-arm64v8


RUN rpm --import https://yum.corretto.aws/corretto.key
RUN curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo
RUN yum install -y java-17-amazon-corretto-devel

RUN curl -L -O https://launcher.mojang.com/v1/objects/e00c4052dac1d59a1188b2aa9d5a87113aaf1122/server.jar
RUN java -Xmx1024M -Xms1024M -jar server.jar nogui
RUN sed -i -e "s/eula=false/eula=true/" eula.txt

COPY function.sh ${LAMBDA_TASK_ROOT}
RUN chmod +x ${LAMBDA_TASK_ROOT}/*.sh

EXPOSE 25565/tcp
EXPOSE 25565/udp


CMD ["./function.sh"]
