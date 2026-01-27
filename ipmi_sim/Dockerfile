FROM alpine:latest AS builder

RUN apk --update --no-cache add \
    git \
    gcc \
    g++ \
    make \
    automake \
    autoconf \
    libtool \
    linux-headers \
    popt-dev \
    ncurses-dev \
    openssl-dev \
    readline-dev \
    glib-dev \
    net-snmp-dev \
    python3-dev \
    pkgconfig

WORKDIR /build
RUN git clone https://github.com/cjh1/openipmi.git && \
    cd openipmi && \
    autoreconf -ivf && \
    ./configure \
        --sysconfdir=/etc \
        --with-openssl \
        --with-glib \
        --enable-shared \
        --disable-static \
        --without-perl \
        --without-python \
        --without-tcl && \
    make -j$(nproc) && \
    make install

FROM alpine:latest

RUN apk --update --no-cache add \
    popt \
    socat \
    tini

COPY --from=builder /usr/local /usr/local

RUN ldconfig /usr/local/lib

RUN mkdir -p /tmp/chassis /ipmi_sim

COPY lan.conf /ipmi_sim/lan.conf
COPY sim.emu /ipmi_sim/sim.emu
COPY entrypoint.sh /ipmi_sim/entrypoint.sh

EXPOSE 623/udp

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/ipmi_sim/entrypoint.sh"]