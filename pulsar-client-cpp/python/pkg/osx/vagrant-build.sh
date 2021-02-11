#!/usr/bin/env bash
#
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
#

if [ "$#" -ne 2 ]; then
    echo "Need to specify git tag and repo as argument"
    exit 1
fi

GIT_TAG=$1
GIT_REPO=$2

set -e -x

rm -rf pulsar
git clone -q --depth 1 --branch $GIT_TAG $GIT_REPO
cd pulsar/pulsar-client-cpp

brew link --force boost
brew link --force protobuf260 || true ## Older images have protobuf 2.6.0 and not linked

#### Python 3
brew unlink python@2
brew unlink boost-python
brew link --force python
brew link --force boost-python3

make clean
rm CMakeCache.txt
PYTHON_INCLUDE_DIR=$(python3 -c "import sysconfig; print(sysconfig.get_path('include'))")
PYTHON_LIB_DIR=$(python3 -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))")
PYTHON_VERSION=$(python3 -c "import sysconfig; print(sysconfig.get_python_version())")
PYTHON_LIBRARY="$PYTHON_LIB_DIR/libpython$PYTHON_VERSION.dylib"
cmake . -DBUILD_TESTS=OFF \
		-DLINK_STATIC=ON  \
		-DPYTHON_LIBRARY=$PYTHON_LIBRARY \
        -DPYTHON_INCLUDE_DIR=$PYTHON_INCLUDE_DIR
make _pulsar -j8
pushd python
python3 setup.py bdist_wheel
popd
