FROM osgeo/gdal:alpine-normal-3.7.2

RUN apk add nodejs yarn git python3 python3-dev py3-pip \
    make bash sqlite-dev zlib-dev geos geos-dev \
    postgresql-libs gcc g++ musl-dev postgresql-dev cairo \
    py3-cairo file

# Download and install Tippecanoe
RUN git clone -b 2.31.0 https://github.com/felt/tippecanoe.git /tmp/tippecanoe && \
    cd /tmp/tippecanoe && \
    make && \
    PREFIX=/usr/local make install && \
    rm -rf /tmp/tippecanoe

WORKDIR /usr/local/src/batch-machine
ADD . /usr/local/src/batch-machine

RUN pip3 install .

CMD python3 test.py
