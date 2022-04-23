FROM golang:1.17 as builder

ARG APP_NAME=halfpipe

ENV LIBRARY_PATH /usr/lib:/usr/local/lib:/usr/lib/instantclient
ENV C_INCLUDE_PATH=/usr/lib/instantclient/sdk/include
ENV NLS_LANG "ENGLISH_UNITED KINGDOM.AL32UTF8"
ENV PKG_CONFIG_PATH=/usr/local/pkgconfig
ENV PATH "$PATH:/go/bin:."
ENV GO111MODULE=on

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends gcc curl git unzip pkg-config libaio1 unixodbc-dev \
    && cd /tmp \
    && mkdir -p "$PKG_CONFIG_PATH" \
    && mkdir -p /root/.ssh \
    && chmod 600 /root/.ssh \
    && ssh-keyscan github.com >> /root/.ssh/known_hosts

# Oracle client (use LD_LIBRARY_PATH from parent image).

RUN cd /tmp \
    && curl -L https://download.oracle.com/otn_software/linux/instantclient/1914000/instantclient-basiclite-linux.x64-19.14.0.0.0dbru.zip -o instantclient-basiclite-linux.zip \
    && curl -L https://download.oracle.com/otn_software/linux/instantclient/1914000/instantclient-sdk-linux.x64-19.14.0.0.0dbru.zip -o instantclient-sdk-linux.zip \
    && for i in instantclient-*-linux.zip; do unzip "$i"; done \
    && rm instantclient*.zip \
    && mv instantclient* /usr/lib/instantclient

ENV PATH "$PATH:/go/src/$APP_NAME/bin:."
RUN mkdir -p src/$APP_NAME/dist
WORKDIR /go/src/$APP_NAME
COPY . .
COPY oci8-docker.pc $PKG_CONFIG_PATH/oci8.pc

#RUN make build-linux
#
################################################################################
################################################################################
#
#FROM debian:stretch-slim
#
#ENV LIBRARY_PATH /usr/lib:/usr/local/lib:/usr/lib/instantclient
#ENV NLS_LANG "ENGLISH_UNITED KINGDOM.AL32UTF8"
#ENV PATH "$PATH:/home/dataops:."
#
#ARG APP_NAME=halfpipe
#ARG USER_AND_GROUP_NAME=dataops
#ARG USER_AND_GROUP_ID=1000
#ARG HOME_DIR=/home/$USER_AND_GROUP_NAME
#
#RUN mkdir -p /home/$USER_AND_GROUP_NAME \
#    && groupadd -g $USER_AND_GROUP_ID "$USER_AND_GROUP_NAME" \
#    && useradd -s /bin/sh -d "$HOME_DIR" -g "$USER_AND_GROUP_NAME" -u $USER_AND_GROUP_ID "$USER_AND_GROUP_NAME" \
#    && chown -R $USER_AND_GROUP_NAME:$USER_AND_GROUP_NAME $HOME_DIR \
#    && apt-get update -y \
#    && apt-get install -y --no-install-recommends unixodbc ca-certificates
#
## Halfpipe
#
#COPY --from=builder /go/src/$APP_NAME/dist/hp /usr/local/bin/
#COPY --from=builder /go/src/$APP_NAME/dist/hp-oracle-plugin.so /usr/local/lib/
#COPY --from=builder /go/src/$APP_NAME/dist/hp-odbc-plugin.so /usr/local/lib/
#COPY LICENSE /home/$USER_AND_GROUP_NAME/
#
#USER $USER_AND_GROUP_NAME
#WORKDIR $HOME_DIR
#
#RUN echo "export PS1='\033[0;96mhalfpipe:\033[1;34m\W \033[0;96m$\033[0m '" >> .bashrc \
#    && echo "alias ll='ls -l'" >> .bashrc \
#    && echo "alias la='ls -la'" >> .bashrc \
#    && echo "hp" >> .bashrc \
#    && echo "echo" >> .bashrc \
#    && chmod 755 .bashrc \
#    && ln -s /usr/local/bin/hp hp
#
#CMD ["bash"]
