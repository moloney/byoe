#!/bin/bash
# Install standalone python for running this package and spack
set -e

os=unknown-linux-gnu
#os=apple-darwin
py_vers=3.9.17
arch=x86_64
#arch=aarch64
pbs_release=20230726

if [ -d ./._internal ] ; then
    echo "The '._internal' dir alreadys exists, exiting"
    exit 1
fi

mkdir ._internal
cd ._internal
# TODO: Make this work on OSX
wget -O python.tar.gz https://github.com/indygreg/python-build-standalone/releases/download/$pbs_release/cpython-${py_vers}+${pbs_release}-${arch}-${os}-install_only.tar.gz
tar -xzf python.tar.gz && rm python.tar.gz
cd -
._internal/python/bin/python3 -m venv byoe_venv
byoe_venv/bin/pip install .
# TODO: User needs to setup config file first... should we run this here?
#byoe_venv/bin/byoe bootstrap