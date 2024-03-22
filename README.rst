==========================
Bring Your Own Environment
==========================

Build and manage modern, reproducible, scientific software stacks anywhere, with a focus 
on HPC.

Combines the package ecosystems of `Spack <https://spack.io/>`_, 
`Conda <https://docs.conda.io/en/latest/>`_, and `PYPI <https://pypi.org/>`_ to provide
a huge amount up-to-date software. The use of Spack allows custom compilation when
needed, including the ability to link to system libraries or compile alternative 
versions of them. Python virutal environments can then be layered on top of spack 
environments to provide access to the full PYPI package ecosystem. Software can be either 
integrated into a base "environment", or installed as an isolated application. The 
latter option is particularly useful for integrating software with conflicting or 
otherwise hard to satisfy dependencies.

Supports compiling software with spack on a Slurm cluster. All packages are cached to 
avoid repeating work, and these caches are available to users to speed up their own 
custom environment builds.

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


Basic Usage
===========

A BYOE repository is a directory where all of the configuration and data (software 
environments, pkgs, etc.) are stored. On single user systems the default location under
the user home directory is probably reasonable provided there is enough space. On 
multi user systems there is generally going to be a single shared repository managed 
by one or more "admins".

If a user configuration doesn't exist, the first time any commands are run you will be
interactively prompted to populate one. The first question will be for the ``base_dir`` 
which provides the location of the BYOE repository.


Using Environments
------------------

If you are on a multi-user system with an existing repository you can start using the
provided environments immediately.

If you want to run a single command in a BYOE environment you can use the ``byoe run``
command.

If instead you want to modify your current shell, you can use the ``byoe activate`` 
command but  the precise usage depends on the shell you are using. For BASH users doing 
``source <(byoe activate)`` is the recommneded approach, while FISH users can do 
``source (byoe activate | psub)``. Finally ``source $(byoe activate --tmp)`` works in 
all shells including CSH but leaves behind temp files.


Building Environments
---------------------

If you are on a single user system or you are an admin for a multi-user system you 
will need to configure and periodically build your BYOE envrionments. The 
``site_conf.yaml`` inside the repository is used for this configuration.

You can run the ``init-dir`` subcommand to prepare the defined ``base_dir``, which is 
useful if you need to prepopulate the contained ``licenses`` directory with any 
software licenses you need to build your environments. 

Running the ``update`` command will build updated versions of all the defined 
environments. This can take a long time, especially on the first run, so check the
corresponding log file under the ``{base_dir}/logs`` directory for progress.

Once you have a working build it is recommended you create a scheduled task to run 
``byoe update`` around the first of each month.


Known Issues
============

Spack doesn't handle certain configuration files being split between different scopes,
and in particular you may need to rename/delete any ``~/.spack/linux/compilers.yaml`` 
file before using ``byoe``.
