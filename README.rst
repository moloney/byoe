==========================
Bring Your Own Environment
==========================

Build and manage modern software stacks, even on old systems, with a focus on HPC.

Uses ``spack`` to build base environments which in turn are used to create python
virtual environments where we can install the most recent versions of any python 
packages. We augment the available applications with ``conda`` packages as needed, 
although these are isolated from the spack base environment and any python virtual 
environments.


Install
=======

Prerequisites
-------------

The only things that must be avaiable ahead of time are ``git`` and a compiler 
toolchain. For example on a Debian/Uuntu system you may need to run:

..
    apt update && apt install git build-essential


Using System Python
-------------------

If you have a recent enough Python installed (>=3.8) you can install this package with 
pip (ideally in a virtual environment or using ``pipx``).

After installing run ``byoe bootstrap`` to complete any bootstrapping.


Not Using System Python
-----------------------

If you don't have or don't want to use a system python install you can clone this 
repo and run ``./bootstrap-no-sys-python.sh`` from the root directory of the repo. 
This will create a virtual environment at ``./venv`` with this package installed.
