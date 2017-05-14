# deploy_cluster
Easily deploy clusters of 2+ nodes with highly available Postgresql. This tool 
is meant to help deploying clusters quickly and reliably in a replicable manner. 

It includes a modified Python translation of 
[PAF](https://github.com/dalibo/PAF), a multi-state Pacemaker Resource Agent 
(RA) for Postgresql.

There are some unit tests and functional tests (eg test PG replication, test 
killing-adding back a slave, test "pcs stanby/unstandby" a slave), but more 
tests are needed to confirm the RA works properly in most scenarios.

# Requirements:

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

# TODO

- Untangle code by eliminating all calls to get_ocf_state.

- Perhaps the RA could try to clean up a crashed master instance by starting 
it and shutting it down. This would be sufficient only if the promoted slave was
all WAL caught up with the master that crashed.

- RA should report a slave that doesn't replicate because its
timeline diverged as down. Perhaps this should bring the whole resource down. 
This should not, however, occur because the RA is suppose to prevent this. 
	
        LOG:  fetching timeline history file for timeline 3 from primary server
        FATAL:  could not start WAL streaming: ERROR:  requested starting point 0/9000000 
        on timeline 2 is not in this server's history
        DETAIL:  This server's history forked from timeline 2 at 0/80001A8.

- Make deployer work on QEMU and VMWare.
