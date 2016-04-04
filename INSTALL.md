# Instructions for setting up GRAPLEr

The steps below explain the process of setting up GRAPLEr in its minimal configuration

**Recommended Operating System:** Ubuntu 14.04 LTS

**Recommended setup:**
+ One machine for [central manager](#central-manager-node) (handles matchmaking and negotiation)
+ One machine for [submit node](#submit-node) running Graple Web Service
+ At least one machine for [worker node](#worker-node)

## Central Manager Node

### Optional Installations
+ open-vm-tools (if using a VMWare hypervisor)
+ openssh-server (for ssh access)

Please perform update and dist-upgrade to install the latest distribution before proceeding.

### Recommended Installations
Install the following with apt-get:
+ vim
+ python-pip
+ git
+ curl
+ jq

### Installing IPOP
_IPOP is necessary if the worker nodes do not share the same network_
+ Download the latest release from Github
+ Extract and modify sample GroupVPN configuration
    - Set the following in CFx: xmpp_username, xmpp_password, xmpp_host
    - Set ip4 in BaseTopologyManager. See default values below:
        - 172.31.0.200 for submit node
        - 172.31.0.100 for central manager
        - 172.31.1.x for workers
+ Make IPOP run at startup and then restart condor service by adding the following in `/etc/rc.local`
```
/home/grapleadmin/ipopbin/startipop.sh
(sleep 15; sudo service condor restart) &
```

### Installing Condor
+ Download the current stable release from [htcondor website](http://research.cs.wisc.edu/htcondor/downloads/)
+ Run the following commands:
NOTE: Copy the appropriate condor_config.local file for each machine
```
sudo dpkg -i package.deb
sudo apt-get -f install
sudo dpkg -i package.deb
cp condor_config.local /etc/condor
service condor start
```

## Worker Node

__Follow the instructions for central manager first and then continue from below__

### Installing R

Run the following commands:
```
sudo sh -c 'echo "deb http://cran.rstudio.com/bin/linux/ubuntu trusty/" >> /etc/apt/sources.list'
gpg --keyserver keyserver.ubuntu.com --recv-key E084DAB9
gpg -a --export E084DAB9 | sudo apt-key add -
update
r-base
```

### Installing GLMr

Install the following with apt-get:
+ netcdf-bin
+ libnetcdf-dev

Run the following R command in superuser mode:
```
    install.packages(c("GLMr", "glmtools"), repos = c("http://owi.usgs.gov/R", getOption("repos")))
```

### Installing glmtools
+ Download [glmtools binary](http://aed.see.uwa.edu.au/research/models/GLM/Pages/getting_started.html)
+ Run the following commands:
```
sudo dpkg -i package.deb
sudo apt-get -f install
sudo dpkg -i package.deb
sudo unzip glmlib.zip /usr/lib
```

## Submit Node
__Follow the instructions for central manager first and then continue from below__

### Installing python dependencies

+ [mongodb-org](https://docs.mongodb.org/manual/tutorial/install-mongodb-on-ubuntu/)

### Install the following with apt-get:
+ python-numpy
+ python-pandas
+ rabbitmq-server

### Install the follwing with pip:
+ Flask
+ Celery
+ pymongo

### Setting up GEMT
Download GEMT repository and create a static folder. Setup the Filters as necessary. The static folder is the working directory of GWS. All experiments and results will be stored in it in a directory whose name is the uid of experiment.

The end result should look like this:
```
grapleService
├── GRAPLE_SCRIPTS
│   ├── CreateWorkingFolders.py
│   ├── Filters
│   │   ├── Filter1.R
│   │   └── Filter2.R
│   ├── Graple
│   │   └── Graple.py
│   ├── Graple.sln
│   ├── ProcessGrapleBatchOutputs.py
│   └── SubmitGrapleBatch.py
└── static
```

The absolute path of grapleService folder should be set as base_working_path in [graple-optimized.py](graple-optimized.py)
`base_working_path = /home/grapleadmin/grapleService/`


### Starting and stopping the service

Run [startGWS.sh](startGWS.sh) or [stopGWS.sh](stopGWS.sh)
