Easily deploy clusters providing highly available resources. This tool 
is meant to help deploying clusters quickly and reliably in a replicable manner. 

In its current form, it is specialized to deploy clusters with highly-available 
Postgresql using a [multi-state Pacemaker resource agent](https://github.com/ulodciv/pgha). 

There are some unit tests and
[functional tests](https://github.com/ulodciv/deploy_cluster/wiki/Functional-Tests),
but more tests are needed to confirm the RA works properly in most scenarios. 
This tool makes it easier to write such functional tests.

# Requirements

- This tool can be run on Linux or Windows
- VirtualBox
- Python >= 3.6 with module Paramiko (python -m pip install paramiko)
- [pgha.py](https://github.com/ulodciv/pgha)

# Usage

1. [Set up a VirtualBox VM](https://docs.google.com/spreadsheets/d/11X_08SDureZ3w_JtihwkJca39ldbIaz9yn7CZgZM9a8/edit?usp=sharing) that has:
    - CentOS/RHEL 7, Debian 9, or Ubuntu Zesty
    - SELinux disabled
    - VirtualBox guest additions
    - Postgresql 9.6 or 10 
    - pcs enabled (disable corosync and pacemaker services)
    - Enable root account (when using Ubuntu)
    
1. Export the VM as an OVA file 
    
1. Update the JSON cluster file as needed (config/pgha_cluster.json)
    - Set the path to pgha.py
    - Set hosts and IP addresses
    - ...

1. Deploy the cluster: 
   
       python src/pgha_deployer.py config/pgha_cluster.json

# [TODO](https://github.com/ulodciv/deploy_cluster/wiki/TODO)
