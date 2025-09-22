==========================
Bring Your Own Environment
==========================

Build and manage rolling releases of modern, reproducible, scientific software stacks, 
with a focus on HPC.

Combines the package ecosystems of `Spack <https://spack.io/>`_, 
`Conda <https://docs.conda.io/en/latest/>`_, `PyPI <https://pypi.org/>`_, and 
`Apptainer <https://apptainer.org/>`_ to provide a huge amount up-to-date software. 
Each `byoe` workspace can define one base `environemnt` (using `spack` + `pypi` or 
`conda`) plus any number of standalone `apps` (using any of the package ecosystems). 
Within the base `environment` all of the software must be able to coexist with no 
conflicting requirements, while `apps` allow an escape hatch for software that is 
hard (or impossible) to install in the base `environment`. However software installed as 
`apps` are not available as libraries that can be imported or linked against, and thus
are only approriate when the the needed functionality from the software is available 
through a CLI / GUI.

Uses a rolling release approach where abstract `environment` and `app` specs are 
periodically concretized using the latest available verisions to create a new `snapshot`. 
Users control their own schedule for updating the default `snapshot`` to use, allowing 
them to balance their desire for stability versus freshness. Older versions of 
`workspaces` can be kept around in full for a period of time for instant access, and 
kept around indefinitely as a set of small text `lock` files that can be used to 
reproduce an `workspace`.

The use of Spack allows custom compilation when needed, including the ability to link to 
system libraries or compile alternative versions of them. While some python packages are
available in Spack most are not, and it only makes sense to use Spack to build Python
packages that need to link to custom built libraries or that may see a performance boost 
from custom compilation. Therefore we allow Python virutal environments to be layered on 
top of spack environments to provide access to the full PyPI package ecosystem. 

The use of Conda does not allow for integration with system libraries or custom 
compilation, but installing Conda packages is generally faster and more reliable that 
Spack since we are just downloading prebuilt binaries. Conda has builtin support for 
installing from `PyPI`.

Supports compiling software with spack on a Slurm cluster. All packages are cached to 
avoid repeating work, and these caches are available to users to speed up their own 
custom environment builds.

Install
=======

Prerequisites
-------------

The only things that must be available ahead of time are ``git`` and a compiler 
toolchain. For example on a Debian/Ubuntu system you may need to run:

..

    apt update && apt install git build-essential

If you want to use Apptainer apps on a system that requires the priviledged (setuid) 
setup you will also need to install and configure that yourself, if a system admin hasn't 
already done so (i.e. if the ``apptainer`` command isn't available).


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
by one or more "admins" (who don't need elevated permissions).

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
all shells including CSH, the only requirement being a writable TMPDIR.


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
