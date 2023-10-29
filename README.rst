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


Not Using System Python
-----------------------

If you don't have or don't want to use a system python install you can clone this 
repo and run ``./bootstrap-no-sys-python.sh`` from the root directory of the repo. 
This will create a virtual environment at ``./byoe_venv`` with this package installed.


Building Environments
---------------------

You will need a configuration file, the ``example_conf`` directory provides an example.
If you are building centralized environments for multiple users, configure the 
``base_dir`` to point to a shared directory.

You can run the ``init-dir`` subcommand to prepare the defined ``base_dir``, which is 
useful if you need to prepopulate the contained ``licesnes`` directory with any 
software licenses you need to build your environments.

Running the ``update-envs`` command will build updated versions of all the defined 
environments. This can take a long time, especially on the first run, so check the
corresponding log file under the ``{base_dir}/logs`` directory for progress.


Known Issues
------------

Spack doesn't handle certain configuration files being split between different scopes,
and in particular you may need to delete any ``~/.spack/linux/compilers.yaml`` file
before using ``byoe``.
