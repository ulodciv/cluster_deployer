# deploy_cluster
Easily deploy clusters of 2+ nodes with resilisent replicated Postgresql.

Works with CentOS/RHEL 7, Debian 9 and Ubuntu Zesty.

Requires Python 3.6 and external module Paramiko.

# Usage

1. Using VirtualBox, set up a VM that has:
    1. the root user enabled (ie with Ubuntu)
    1. SELinux disabled
    1. VirtualBox guest additions
    1. Postgresql server 9.6 
    1. pcs enabled (disable corosync and pacemaker services)

1. Export the VM as an OVA file 
    
1. Update the JSON cluster file as needed (cluster.json)

1. Deploy the cluster: 
   
        python src\app.py cluster.json

# TODO

- Issues:
	- RA should block slave instances that fail to stream from Master, at least with timeline fork issues, eg:
		< 2017-04-11 22:54:41.032 CEST > LOG:  fetching timeline history file for timeline 3 from primary server
		< 2017-04-11 22:54:41.034 CEST > FATAL:  could not start WAL streaming: ERROR:  requested starting point 0/9000000 on timeline 2 is not in this server's history
        DETAIL:  This server's history forked from timeline 2 at 0/80001A8.
	- Perhaps the RA could try to clean up a crashed instance by starting it and shutting it down

- Make deployer work on VMWare.