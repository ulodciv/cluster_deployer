# deploy_cluster
Easily deploy clusters of 2+ nodes with resilisent replicated Postgresql. The 
purpose of this application is to assist in the preparation of clusters to 
run tests and validate that the postgresl resource agent works properly. 

Runs on Linux or Windows with:
- Python 3.6 using external module Paramiko.
- VirtualBox
- A pre-configured OVA running:
    - CentOS/RHEL 7
    - Debian 9
    - Ubuntu Zesty

# Usage

1. Set up a VirtualBox VM that has:
    - SELinux disabled
    - VirtualBox guest additions
    - Postgresql 9.6 
    - pcs enabled (disable corosync and pacemaker services)
    - Enable root account (when using Ubuntu)
    
1. Export the VM as an OVA file 
    
1. Update the JSON cluster file as needed (config\cluster.json)

1. Deploy the cluster: 
   
        python deployer\app.py config\cluster.json

# TODO

- Issues:

	- Perhaps the RA could try to clean up a crashed master instance by starting 
	it and shutting it down

 	- RA should (optionally ?) report a non replicating slave as down. This can 
	possibly be done with replication slots and inspecting the ouptut of 
            
            SELECT * FROM pg_replication_slots
             
	- RA should block slave instances that fail to stream from Master, at least 
	with timeline fork issues, eg:
	
            LOG:  fetching timeline history file for timeline 3 from primary server
            FATAL:  could not start WAL streaming: ERROR:  requested starting point 0/9000000 
            on timeline 2 is not in this server's history
            DETAIL:  This server's history forked from timeline 2 at 0/80001A8.

- Make deployer work on QEMU and VMWare.
