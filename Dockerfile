FROM ghcr.io/osgeo/gdal:alpine-normal-3.11.0

RUN apk add --no-cache nodejs yarn git python3-dev py3-pip \
    make sqlite-dev zlib-dev geos-dev \
    gcc g++ musl-dev postgresql-dev cairo \
    py3-cairo file

# Download and install Tippecanoe
RUN git clone -b 2.31.0 https://github.com/felt/tippecanoe.git /tmp/tippecanoe && \
    cd /tmp/tippecanoe && \
    make && \
    PREFIX=/usr/local make install && \
    rm -rf /tmp/tippecanoe

WORKDIR /usr/local/src/batch-machine
ADD . /usr/local/src/batch-machine

RUN pip3 install --break-system-packages .

CMD python3 test.py
