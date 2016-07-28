Combine - the Combine Web Harvester
===================================

Combine is a generic web harvesting architecture.

The default location of this repository is: 

git@kas.ewi.utwente.nl:smartcopi/combine.git


Set up
------

To run combine, make sure the environment satisfies the following requirements:

  * Python 3.4+
  * Install the packages specification from `requirements.txt` (most easily with `pip install -r requirements.txt`)
  * Have a Postgress Database available with a, preferrable empty, database
  * copy ./combine.cfg to ./combine.local.cfg
  * Enter the Postgres database login credentials

Run
---
  * Look at the create_schedule() function in ./run.py how to specify an execution job
  * New node handling modules must be created in de ./modules/ Look at the copy method for an a simple example which just creates a copy of an object.
  * The machinery is run by executing ./run.py


Contributing
------------

This repository is laid out in packages. Only configuration, this readme, the python requirements file and the run shell script should be in the repository root.

Files that match the `*.local.*` pattern are not committed to allow local configuration and shell scripts to be placed in directly in your working directory.
