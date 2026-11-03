FROM ghcr.io/osgeo/gdal:alpine-normal-3.13.3@sha256:102f7f57fea5ce2cb4cb5a1deb060b78ab1f5a77232663a84641d4b7bf2e1aa3

RUN apk add --no-cache nodejs yarn git python3-dev py3-pip \
    make sqlite-dev zlib-dev geos-dev \
    gcc g++ musl-dev postgresql-dev cairo \
    py3-cairo file

# Download and install Tippecanoe
RUN git clone -b 2.79.0 https://github.com/felt/tippecanoe.git /tmp/tippecanoe && \
    cd /tmp/tippecanoe && \
    make && \
    PREFIX=/usr/local make install && \
    rm -rf /tmp/tippecanoe

WORKDIR /usr/local/src/batch-machine
ADD . /usr/local/src/batch-machine

RUN pip3 install --break-system-packages .

CMD python3 test.py
