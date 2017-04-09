# deploy_cluster
Easily deploy clusters of 2+ nodes with resilisent replicated Postgresql.

Works with CentOS/RHEL 7, Debian 9 and Ubuntu Zesty.

Requires Python 3.6 and external module Paramiko.

# Usage

1. Using VirtualBox, set up a VM that has:
    1. root user enabled (ie with Ubuntu)
    1. SELinux disabled
    1. VirtualBox guest additions
    1. Postgresql server 9.6 
    1. pcs enabled (disable corosync and pacemaker services)

1. Export the VM as an OVA file 
    
1. Update the JSON cluster file as needed (cluster.json)

1. Deploy the cluster: 
   
        python src\app.py cluster.json

# TODO
Make work on VMWare.