# Instructions for setting up GrapleWebService

The steps below explain the process of setting up GWS on a Linux machine.

Recommended Operating System: Ubuntu Linux 14.04 LTS

## Optional Installations

+ open-vm-tools (if using a VMWare hypervisor)
+ openssh-server (for ssh access)

Please perform update and dist-upgrade to install the latest distribution before proceeding.

## Recommended Installations

Install the following with aptitude:
+ vim
+ python-pip
+ git
+ curl
+ jq

## Installing Condor
+ Download current stable release the [htcondor website](http://research.cs.wisc.edu/htcondor/downloads/)
+ Run the following commands
```
sudo dpkg -i package.deb
sudo apt-get -f install
sudo dpkg -i package.deb
cp condor_config.local /etc/condor
service condor start
```

## Installing python dependencies

+ [mongodb-org](https://docs.mongodb.org/manual/tutorial/install-mongodb-on-ubuntu/)

Install the following with aptitude:
+ python-numpy
+ python-pandas
+ rabbitmq-server

Install the follwing with pip:
+ Flask
+ Celery
+ pymongo

## Starting and stopping the service

Run [startGWS.sh](startGWS.sh) or [stopGWS.sh](stopGWS.sh)
