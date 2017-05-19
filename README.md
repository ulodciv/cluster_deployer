Easily deploy clusters of 2+ nodes with highly available Postgresql. This tool 
is meant to help deploying clusters quickly and reliably in a replicable manner. 

It includes a modified Python translation of 
[PAF](https://github.com/dalibo/PAF), a multi-state Pacemaker Resource Agent 
(RA) for Postgresql.

There are some unit tests and functional tests (eg test PG replication, test 
killing-adding back a slave, test "pcs stanby/unstandby" a slave), but more 
tests are needed to confirm the RA works properly in most scenarios.

# Requirements (for the deployer):

- Linux or Windows
- VirtualBox
- Python >= 3.6 
- Python module Paramiko (python -m pip install paramiko)

# Usage

1. Set up a VirtualBox VM that has:
    - CentOS/RHEL 7, Debian 9, or Ubuntu Zesty
    - SELinux disabled
    - VirtualBox guest additions
    - Postgresql 9.6 
    - pcs enabled (disable corosync and pacemaker services)
    - Enable root account (when using Ubuntu)
    
1. Export the VM as an OVA file 
    
1. Update the JSON cluster file as needed (config\cluster.json)

1. Deploy the cluster: 
   
        python deployer\app.py config\cluster.json

# [TODO](https://github.com/ulodciv/deploy_cluster/wiki/TODO)
