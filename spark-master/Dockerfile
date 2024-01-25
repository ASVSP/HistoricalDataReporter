FROM bde2020/spark-master:3.0.1-hadoop3.2

RUN apk update && apk add openssh-server && apk add vim
RUN mkdir /var/run/sshd

RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config
RUN echo "root:asvsp" | chpasswd
RUN echo "export CORE_CONF_fs_defaultFS=hdfs://namenode:9000" >> /etc/profile
# ?
RUN ssh-keygen -A

EXPOSE 22