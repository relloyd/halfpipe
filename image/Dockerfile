ARG HP_VERSION
ARG ORA_VERSION

FROM relloyd/halfpipe-oracle-${ORA_VERSION}-no-oci:${HP_VERSION}

USER root

RUN apt-get update -y \
    && apt-get install -y alien curl less vim libaio1 ca-certificates unzip glibc-source groff \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm awscliv2.zip

# Oracle client

RUN curl -L https://download.oracle.com/otn_software/linux/instantclient/195000/oracle-instantclient19.5-basiclite-19.5.0.0.0-1.x86_64.rpm -o /tmp/oracle-instantclient.rpm \
    && for f in /tmp/oracle-*.rpm; do alien -i $f; done \
    && rm /tmp/oracle-instantclient.rpm
ENV ORACLE_HOME=/usr/lib/oracle/$ORA_VERSION/client64
ENV LD_LIBRARY_PATH=/usr/lib:/usr/local/lib:/usr/lib/oracle/$ORA_VERSION/client64/lib

# Kubectl

ARG K8S_VERSION
ENV K8S_VERSION=${K8S_VERSION}
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/${K8S_VERSION}/bin/linux/amd64/kubectl \
    && chmod +x ./kubectl \
    && mv ./kubectl /usr/local/bin/kubectl \
    && echo "alias k=kubectl" >> .bashrc

# K9s

RUN mkdir -p /usr/local/k9s \
    && cd /usr/local/k9s \
    && curl -L https://github.com/derailed/k9s/releases/download/0.9.2/k9s_0.9.2_Linux_x86_64.tar.gz -o k9s.tar.gz \
    && tar xvzf k9s.tar.gz \
    && rm k9s.tar.gz \
    && chmod +x k9s \
    && ln -s /usr/local/k9s/k9s /usr/local/bin/k9s \
    && echo "export TERM=xterm-256color" >> /home/dataops/.bashrc

USER dataops

# Set the halfpipe version so scripts can use it as a default.

ARG HP_VERSION
ENV HP_VERSION=${HP_VERSION}

# Scripts

COPY --chown=dataops:dataops kubes/template/ /home/dataops/.template/
COPY --chown=dataops:dataops kubes/create-k8s-pipe-job /home/dataops/
COPY --chown=dataops:dataops configure.sh /home/dataops/
COPY --chown=dataops:dataops .template-variables.env /home/dataops/

WORKDIR /home/dataops
