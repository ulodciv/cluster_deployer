# deploy_cluster
Easily deploy clusters of 2+ nodes with resilisent replicated Postgresql. The 
purpose of this application is to assist in the preparation of clusters to 
run tests and validate that the postgresl resource agent works properly. 

Runs using VirtualBox on Windows, and probably on UNIX-likes with minor changes.

Requires Python 3.6 using external module Paramiko.

Deploys nodes running CentOS/RHEL 7, Debian 9 or Ubuntu Zesty.

# Usage

1. Using VirtualBox on Windows, set up a VM that has:
    1. the root user enabled (ie with Ubuntu)
    1. SELinux disabled
    1. VirtualBox guest additions
    1. Postgresql server 9.6 
    1. pcs enabled (disable corosync and pacemaker services)

1. Export the VM as an OVA file 
    
1. Update the JSON cluster file as needed (config\cluster.json)

1. Deploy the cluster: 
   
        python deployer\app.py config\cluster.json

# TODO

- Issues:

	- Perhaps the RA could try to clean up a crashed master instance by starting 
	it and shutting it down

    - In all cases, RA should cancel promote of a slave that has a diverging timeline.
 
    - RA should report a non replicating slave as down, somehow. This can 
    possibly be done with replication slots and inspecting the ouptut of 
            
            SELECT * FROM pg_replication_slots
             
	- RA should block slave instances that fail to stream from Master, at least 
	with timeline fork issues, eg:
	
            LOG:  fetching timeline history file for timeline 3 from primary server
            FATAL:  could not start WAL streaming: ERROR:  requested starting point 0/9000000 
            on timeline 2 is not in this server's history
            DETAIL:  This server's history forked from timeline 2 at 0/80001A8.
            

- Make deployer work on Linux (shouldn't require too much work)

- Make deployer work on QEMU and VMWare.