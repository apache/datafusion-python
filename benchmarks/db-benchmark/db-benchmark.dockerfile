# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM ubuntu:22.04
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETPLATFORM

# This section is based on https://github.com/duckdblabs/db-benchmark/blob/master/_utils/repro.sh

RUN apt-get -qq update
RUN apt-get -qq -y upgrade
RUN apt-get -qq install -y apt-utils

RUN apt-get -qq install -y lsb-release software-properties-common wget curl vim htop git byobu libcurl4-openssl-dev libssl-dev
RUN apt-get -qq install -y libfreetype6-dev
RUN apt-get -qq install -y libfribidi-dev
RUN apt-get -qq install -y libharfbuzz-dev
RUN apt-get -qq install -y git
RUN apt-get -qq install -y libxml2-dev
RUN apt-get -qq install -y make
RUN apt-get -qq install -y libfontconfig1-dev
RUN apt-get -qq install -y libicu-dev pandoc zlib1g-dev libgit2-dev libcurl4-openssl-dev libssl-dev libjpeg-dev libpng-dev libtiff-dev
# apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
RUN add-apt-repository "deb [arch=amd64,i386] https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/"

RUN apt-get -qq install -y r-base-dev virtualenv

RUN cd /usr/local/lib/R && \
  chmod o+w site-library

RUN cd / && \
    git clone https://github.com/duckdblabs/db-benchmark.git

WORKDIR /db-benchmark

RUN mkdir -p .R && \
    echo 'CFLAGS=-O3 -mtune=native' >> .R/Makevars && \
    echo 'CXXFLAGS=-O3 -mtune=native' >> .R/Makevars

RUN cd pydatatable && \
  virtualenv py-pydatatable --python=/usr/bin/python3.10
RUN cd pandas && \
  virtualenv py-pandas --python=/usr/bin/python3.10
RUN cd modin && \
  virtualenv py-modin --python=/usr/bin/python3.10

RUN Rscript -e 'install.packages(c("jsonlite","bit64","devtools","rmarkdown"), dependencies=TRUE, repos="https://cloud.r-project.org")'

SHELL ["/bin/bash", "-c"]

RUN source ./pandas/py-pandas/bin/activate && \
    python3 -m pip install --upgrade psutil && \
    python3 -m pip install --upgrade pandas && \
    deactivate

RUN source ./modin/py-modin/bin/activate && \
    python3 -m pip install --upgrade modin && \
    deactivate

RUN source ./pydatatable/py-pydatatable/bin/activate && \
    python3 -m pip install --upgrade git+https://github.com/h2oai/datatable && \
    deactivate

## install dplyr
#RUN Rscript -e 'devtools::install_github(c("tidyverse/readr","tidyverse/dplyr"))'

# install data.table
RUN Rscript -e 'install.packages("data.table", repos="https://rdatatable.gitlab.io/data.table/")'

## generate data for groupby 0.5GB
RUN Rscript _data/groupby-datagen.R 1e7 1e2 0 0
RUN #Rscript _data/groupby-datagen.R 1e8 1e2 0 0
RUN #Rscript _data/groupby-datagen.R 1e9 1e2 0 0

RUN mkdir data && \
  mv G1_1e7_1e2_0_0.csv data/

# set only groupby task
RUN echo "Changing run.conf and _control/data.csv to run only groupby at 0.5GB" && \
    cp run.conf run.conf.original && \
    sed -i 's/groupby join groupby2014/groupby/g' run.conf && \
    sed -i 's/data.table dplyr pandas pydatatable spark dask clickhouse polars arrow duckdb/data.table dplyr duckdb/g' run.conf && \
    sed -i 's/DO_PUBLISH=true/DO_PUBLISH=false/g' run.conf

## set sizes
RUN mv _control/data.csv _control/data.csv.original && \
    echo "task,data,nrow,k,na,sort,active" > _control/data.csv && \
    echo "groupby,G1_1e7_1e2_0_0,1e7,1e2,0,0,1" >> _control/data.csv

RUN #./dplyr/setup-dplyr.sh
RUN #./datatable/setup-datatable.sh
RUN #./duckdb/setup-duckdb.sh

# END OF SETUP

RUN python3 -m pip install --upgrade pandas
RUN python3 -m pip install --upgrade polars psutil
RUN python3 -m pip install --upgrade datafusion

# Now add our solution
RUN rm -rf datafusion-python 2>/dev/null && \
    mkdir datafusion-python
ADD benchmarks/db-benchmark/*.py datafusion-python/
ADD benchmarks/db-benchmark/run-bench.sh .

ENTRYPOINT [ "/db-benchmark/run-bench.sh" ]