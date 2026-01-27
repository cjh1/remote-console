#
# MIT License
#
# (C) Copyright 2021-2022, 2025 Hewlett Packard Enterprise Development LP
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# Dockerfile for the console-data service

### goreleaser Stage ###
### Assume goreleaser has already compiled the binary and written it to ./remote-console

FROM ubuntu:24.04 AS ubuntu-goreleaser

RUN apt -y update
RUN apt -y install conman less vim ssh jq tar procps inotify-tools

COPY remote-console /app/
COPY scripts/conman.conf.tmpl /app/conman.conf.tmpl
COPY scripts/ssh-key-console /usr/bin/
COPY scripts/ssh-pwd-console /usr/bin/
COPY configs /app/configs

RUN chown -Rv 65534:65534 /app /etc/conman.conf
USER 65534:65534

RUN echo 'alias ll="ls -l"' > /app/bashrc
RUN echo 'alias vi="vim"' >> /app/bashrc

ENTRYPOINT ["/app/remote-console"]


# Build Stage: Build the Go binary
FROM golang:1.24-bookworm AS builder

RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends build-essential

# Configure go env
ENV GOPATH=/usr/local/golib
RUN export GOPATH=$GOPATH
RUN go env -w GO111MODULE=auto

# Copy source files
COPY cmd $GOPATH/src/github.com/OpenCHAMI/remote-console/v2/cmd
COPY configs configs
COPY scripts scripts
COPY internal $GOPATH/src/github.com/OpenCHAMI/remote-console/v2/internal
COPY go.mod $GOPATH/src/github.com/OpenCHAMI/remote-console/v2/go.mod
COPY go.sum $GOPATH/src/github.com/OpenCHAMI/remote-console/v2/go.sum

# Build the image
RUN set -ex && go build -C $GOPATH/src/github.com/OpenCHAMI/remote-console/v2/cmd/remote-console -v -o /usr/local/bin/remote-console

### Final Stage ###
FROM ubuntu:24.04 AS final

# Install needed packages
RUN set -eux \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ipmitool \
        libfreeipmi17 \
        libipmiconsole2 \
        iputils-ping \
        coreutils \
        conman \
        libcap2-bin \
        expect \
        openssh-client \
        sshpass \
        vim \
        bash \
        jq \
        inotify-tools \
        logrotate \
    && rm -rf /var/lib/apt/lists/*

# Copy in the needed files
COPY --from=builder /usr/local/bin/remote-console /app/
COPY scripts/conman.conf.tmpl /app/conman.conf.tmpl
COPY scripts/ssh-key-console /usr/bin/
COPY scripts/ssh-pwd-console /usr/bin/
COPY configs /app/configs

# Aliases
RUN echo 'alias ll="ls -l"' >> /root/.bashrc
RUN echo 'alias vi="vim"' >> /root/.bashrc
RUN chmod +775 /usr/bin/ssh-key-console /usr/bin/ssh-pwd-console

# Create log directories and set ownership to nobody (UID/GID 65534)
RUN mkdir -p /var/log/conman/ /var/log/conman.old/ \
    && chown -Rv 65534:65534 /app /var/log/conman/ /var/log/conman.old/

USER 65534:65534

ENTRYPOINT ["/app/remote-console"]
