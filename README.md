# deploy_cluster
Easily deploy clusters of 2+ nodes with resilisent replicated Postgresql.

Works with CentOS/RHEL 7, Debian 9 and Ubuntu Zesty.

Requires Python 3.6 and external module Paramiko.

# Usage

1. Prepare and export a VM in the OVA form. This VM needs:
    1. An active root (ie with Ubuntu)
    2. SELinux disabled
    3. VirtualBox guest additions
    4. Postgresql server 9.6 installed
    5. pcs installed and enabled (disable corosync and pacemaker services)
    
2. Prepare JSON cluster file (eg cluster.json)

2. Deploy the cluster: 
   
        python src\app.py cluster.json

# TODO
Make work on VMWare.