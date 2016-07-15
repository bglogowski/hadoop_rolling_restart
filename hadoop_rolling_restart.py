#!/usr/bin/env python3.5
"""
Python script to restart Hadoop services via Ambari's REST API
"""

import re
import json
import time
import logging
import socket
import os.path
import click
import requests
import paramiko


# Set constants for various aspects of the restarts

HTTP_RETRY_DELAY = 5
MAX_RETRY_BEFORE_RESTART = 10
NAMENODE_RESTART_DELAY = 120
SAFEMODE_RETRY_DELAY = 30
DATANODE_RESTART_DELAY = 120
JOURNALNODE_RESTART_DELAY = 120
RESOURCEMANAGER_RESTART_DELAY = 120
NODEMANAGER_RESTART_DELAY = 120
HBASEMASTER_RESTART_DELAY = 120
REGIONSERVER_RESTART_DELAY = 120
PHXQUERYSERVER_RESTART_DELAY = 120
ZOOKEEPER_RESTART_DELAY = 120
SPARKTHRIFT_RESTART_DELAY = 120
KAFKA_RESTART_DELAY = 120

# Dictionary of services
# (Only for reference at this time)
SERVICES = {
    'HDFS': [
        'NAMENODE',
        'DATANODE',
        'JOURNALNODE',
        'ZKFC',
        'NFS_GATEWAY',
        'HDFS_CLIENT'
    ],
    'HBASE': [
        'HBASE_MASTER',
        'HBASE_REGIONSERVER',
        'PHOENIX_QUERY_SERVER',
        'HBASE_CLIENT'
    ],
    'YARN': [
        'RESOURCEMANAGER',
        'NODEMANAGER',
        'APP_TIMELINE_SERVER',
        'YARN_CLIENT'
    ],
    'MR2': [
        'HISTORYSERVER',
        'MAPREDUCE2_CLIENT'
    ],
    'TEZ': [
        'TEZ_CLIENT'
    ],
    'HIVE': [
        'HIVE_METASTORE',
        'HIVE_SERVER',
        'WEBHCAT_SERVER',
        'MYSQL_SERVER',
        'HCAT',
        'HIVE_CLIENT'
    ],
    'PIG': [
        'PIG'
    ],
    'OOZIE': [
        'OOZIE_SERVER',
        'OOZIE_CLIENT'
    ],
    'ZOOKEEPER': [
        'ZOOKEEPER_SERVER',
        'ZOOKEEPER_CLIENT'
    ],
    'SPARK': [
        'SPARK_JOBHISTORYSERVER',
        'SPARK_THRIFTSERVER',
        'SPARK_CLIENT'
    ],
    'FALCON': [
        'FALCON_SERVER',
        'FALCON_CLIENT'
    ],
    'KAFKA': [
        'KAFKA_BROKER'
    ],
    'AMS': [
        'METRICS_COLLECTOR',
        'METRICS_MONITOR'
    ],
    'SLIDER': [
        'SLIDER'
    ],
    'SQOOP': [
        'SQOOP'
    ],
    'RANGER': [
        'RANGER_ADMIN',
        'RANGER_KMS_SERVER',
        'RANGER_USERSYNC',
    ],
    'MAHOUT': [
        'MAHOUT'
    ],
    'KNOX': [
        'KNOX_GATEWAY'
    ],
    'STORM': [
        'NIMBUS',
        'STORM_REST_API',
        'STORM_UI_SERVER',
        'DRPC_SERVER',
        'SUPERVISOR'
    ]
}


# Configure the syslog settings

FORMAT = "%(asctime)s %(levelname)s %(message)s"

logging.basicConfig(format=FORMAT, level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class Ambari(object):
    """
    Class which defines an Ambari Server Object
    """
    def __init__(self, name, username, password, cluster, domain, port):
        """
        """
        self.name = name
        self.domain = domain
        self.fqdn = name + '.' + domain

        logging.debug('=> Initializing Ambari server: %s', self.fqdn)

        self.port = port
        self.cluster = cluster

        self.url = 'http://{0}:{1}/api/v1/clusters/{2}'.format(self.fqdn, self.port, self.cluster)

        self.username = username
        self.password = password

        self.headers = {'X-Requested-By': 'hadoop.py'}

        self.__set_hosts()

        logging.debug('=> Initialization of Ambari complete.')


    def get_items(self, uri):
        """
        """
        try:
            req = requests.get(
                self.url + uri,
                auth=(
                    self.username,
                    self.password
                ),
                headers=self.headers
            ).content
            return json.loads(req.decode('utf8'))['items']
        except Exception as exception:
            logging.warning('%s: JSON data not returned from %s, Retrying...', exception.__class__.__name__, self.url + uri)
            time.sleep(HTTP_RETRY_DELAY)
            return self.get_items(uri)

    def execute(self, url, payload):
        """
        Submits commands which are run serially
        """
        logging.debug('Attempting HTTP PUT to %s with payload: %s', url, payload)
        try:
            req = requests.put(
                url,
                auth=(
                    self.username,
                    self.password
                ),
                headers=self.headers,
                data=json.dumps(payload)
            )
            if req.status_code == 202:
                return 'OK'
            else:
                logging.error('HTTP PUT to %s returned: %s', url, json.loads(req.content.decode('utf8')))
                time.sleep(HTTP_RETRY_DELAY)
                return self.execute(url, payload)
        except Exception as exception:
            logging.warning('%s: JSON data not returned from %s, Retrying...', exception.__class__.__name__, url)
            time.sleep(HTTP_RETRY_DELAY)
            return self.execute(url, payload)

    def queue(self, url, payload):
        """
        Queues commands which can be run in parallel
        """
        logging.debug('Attempting HTTP POST to %s with payload: %s', url, payload)
        try:
            req = requests.post(
                url,
                auth=(
                    self.username,
                    self.password
                ),
                headers=self.headers,
                data=json.dumps(payload)
            )
            if req.status_code == 202:
                return 'OK'
            else:
                logging.error('HTTP POST to %s returned contents: %s', url, json.loads(req.content.decode('utf8')))
                time.sleep(HTTP_RETRY_DELAY)
                return self.queue(url, payload)
        except Exception as exception:
            logging.warning('%s: JSON data not returned from %s, Retrying...', exception.__class__.__name__, url)
            time.sleep(HTTP_RETRY_DELAY)
            return self.queue(url, payload)

    def __set_hosts(self):
        """
        Private method which auto-discovers which hosts are managed by Ambari
        """
        hosts = []
        uri = '/hosts'
        for item in self.get_items(uri):
            name = item['Hosts']['host_name'].split('.')[0]
            domain = '.'.join(item['Hosts']['host_name'].split('.')[1:])
            logging.debug('%s: Found %s', self.__class__.__name__, name + '.' + domain)
            hosts.append(Host(name, domain, self))
        self.hosts = hosts


class Host(object):
    """
    Class which defines an Ambari-managed Host Object
    """
    def __init__(self, name, domain, ambari):
        """
        """
        self.name = name
        self.domain = domain
        self.fqdn = name + '.' + domain
        logging.debug('=> Initializing Host: %s...', self.fqdn)

        self.cluster = ambari.cluster
        self.ambari = ambari

        self.uri = '/hosts/' + self.fqdn
        self.url = self.ambari.url + self.uri

        self.services = []
        self.namenode = False
        self.journalnode = False
        self.datanode = False
        self.zkfc = False
        self.nfs_gateway = False
        self.hbasemaster = False
        self.regionserver = False
        self.phoenix = False
        self.resourcemanager = False
        self.nodemanager = False
        self.apptimeline = False
        self.jobhistory = False
        self.zookeeper = False
        self.oozie = False
        self.sparkhistory = False
        self.sparkthrift = False
        self.kafka = False

        self.__set_properties()

        logging.debug('=> Initialization for Host complete.')

    def ssh_with_password(self, username, password, command):
        """
        """
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(self.fqdn, username=username, password=password)
            stdin, stdout, stderr = ssh.exec_command(command)
            logging.debug('SSH to %s succeeded.', self.fqdn)
            return True
        except Exception as exception:
            logging.error('%s: SSH to %s failed with command: %s', exception.__class__.__name__, self.fqdn, command)
            return False

    def ssh_with_key(self, private_key, command):
        """
        """
        if os.path.isfile(private_key):
            key = paramiko.RSAKey.from_private_key_file(private_key)
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                ssh.connect(self.fqdn, pkey=key)
                stdin, stdout, stderr = ssh.exec_command(command)
                logging.debug('SSH to %s succeeded.', self.fqdn)
                return True
            except Exception as exception:
                logging.error('%s: SSH to %s failed with command: %s', exception.__class__.__name__, self.fqdn, command)
                return False
        else:
            logging.error('SSH private key %s not found.', private_key)
            return False

    def __set_properties(self):
        """
        """
        uri = self.uri + '/host_components'
        for component in self.ambari.get_items(uri):
            service = component['HostRoles']['component_name']
            self.services.append(service)

            # Anything can run on any server, check them all!

            logging.debug('%s: Found %s', self.__class__.__name__, service)

            # HDFS services
            if service == 'NAMENODE':
                self.namenode = True
            if service == 'JOURNALNODE':
                self.journalnode = True
            if service == 'DATANODE':
                self.datanode = True
            if service == 'ZKFC':
                self.zkfc = True
            if service == 'NFS_GATEWAY':
                self.nfs_gateway = True

            # HBase services
            if service == 'HBASE_MASTER':
                self.hbasemaster = True
            if service == 'HBASE_REGIONSERVER':
                self.regionserver = True
            if service == 'PHOENIX_QUERY_SERVER':
                self.phoenix = True

            # YARN Services
            if service == 'RESOURCEMANAGER':
                self.resourcemanager = True
            if service == 'NODEMANAGER':
                self.nodemanager = True
            if service == 'APP_TIMELINE_SERVER':
                self.apptimeline = True

            # Other Hadoop services
            if service == 'HISTORYSERVER':
                self.jobhistory = True
            if service == 'ZOOKEEPER_SERVER':
                self.zookeeper = True
            if service == 'OOZIE_SERVER':
                self.oozie = True
            if service == 'SPARK_JOBHISTORYSERVER':
                self.sparkhistory = True
            if service == 'SPARK_THRIFTSERVER':
                self.sparkthrift = True
            if service == 'KAFKA_BROKER':
                self.kafka = True


class HadoopHost(object):
    """
    Class which defines a standard Hadoop Host Object
    """
    def __init__(self, host, port):
        """
        """
        try:
            self.description
        except NameError:
            self.description = "Service"

        self.name = host.name
        self.domain = host.domain
        self.fqdn = host.name + '.' + host.domain
        self.port = port
        self.ambari = host.ambari
        self.ambari_url = self.ambari.url

        self.start_command = {
            "RequestInfo": {
                "context": "Start {0} via REST".format(self.description)
            },
            "Body": {
                "HostRoles": {
                    "state": "STARTED"
                }
            }
        }

        self.stop_command = {
            "RequestInfo": {
                "context": "Stop {0} via REST".format(self.description)
            },
            "Body": {
                "HostRoles": {
                    "state": "INSTALLED"
                }
            }
        }

    def stop(self):
        """
        Public method to stop the service
        """
        logging.info('Stopping %s on %s...', self.description, self.fqdn)
        return self.ambari.execute(self.ambari_url, self.stop_command)

    def start(self):
        """
        Public method to start the service
        """
        logging.info('Starting %s on %s...', self.description, self.fqdn)
        return self.ambari.execute(self.ambari_url, self.start_command)

    def tcp_port_open(self):
        """
        Public method to check if a TCP port is open and warn if it is not
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((self.fqdn, self.port))

        if result == 0:
            logging.debug('TCP port %s:%i is open.', self.fqdn, self.port)
            return True
        else:
            logging.warning('TCP port %s:%i is closed.', self.fqdn, self.port)
            return False

    def tcp_port_closed(self):
        """
        Public method to check if a TCP port is closed
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((self.fqdn, self.port))

        if result == 0:
            logging.debug('TCP port %s:%i is open.', self.fqdn, self.port)
            return False
        else:
            logging.debug('TCP port %s:%i is closed.', self.fqdn, self.port)
            return True

    def is_healthy(self):
        """
        Public method to query the health of the service
            - Defaults to a TCP port check
            - Should be overridden if there are more sophisticated checks available
        """
        return self.tcp_port_open()


class HadoopService(object):
    """
    Class which defines service-wide properties and actions
    """
    def __init__(self, host, service):
        """
        """
        self.service = service
        self.host = host
        self.ambari = host.ambari

        request_uri = '/requests/'
        self.ambari_url = self.ambari.url + request_uri

        self.refresh_configs = {
            "RequestInfo": {
                "command": "RESTART",
                "context": "Restart {0} Client on {1}".format(self.service, self.host.fqdn),
                "operation_level":{
                    "level": "HOST",
                    "cluster_name": "{0}".format(self.host.cluster)
                    }
                },
            "Requests/resource_filters": [
                {
                    "service_name": "{0}".format(self.service),
                    "component_name": "{0}_CLIENT".format(self.service),
                    "hosts": "{0}".format(self.host.fqdn)
                }
            ]
        }

        self.restart_command = {
            "RequestInfo": {
                "command": "RESTART",
                "context": "Restart {0} on {1}".format(self.service, self.host.fqdn),
                "operation_level":{
                    "level": "HOST",
                    "cluster_name": "{0}".format(self.host.cluster)
                    }
                },
            "Requests/resource_filters": [
                {
                    "service_name": "{0}".format(self.service),
                    "component_name": "{0}".format(self.service),
                    "hosts": "{0}".format(self.host.fqdn)
                }
            ]
        }

    def refresh(self):
        """
        """
        logging.info('Refreshing client configs for %s on %s...', self.service, self.host.fqdn)
        return self.ambari.queue(self.ambari_url, self.refresh_configs)

    def restart(self):
        """
        """
        logging.info('Restarting %s on %s...', self.service, self.host.fqdn)
        return self.ambari.queue(self.ambari_url, self.restart_command)


class JmxHadoopHost(HadoopHost):
    """
    Parent Class for various types of Hadoop hosts that support
    JMX over HTTP through a standard TCP port
    """
    def __init__(self, host, port):
        """
        """
        super(JmxHadoopHost, self).__init__(host, port)
        self.jmx_url = 'http://{0}.{1}:{2}/jmx?qry=Hadoop:*'.format(self.name, self.domain, str(port))
        self.jmx_dict = self.get_jmx_dict(self.jmx_url)


    def get_jmx(self, url):
        """
        Connects to the service to get JMX data in JSON format
        """
        try:
            return requests.get(url).content
        except Exception as exception:
            logging.warning('%s: JMX data not returned from %s, Retrying...', exception.__class__.__name__, url)
            time.sleep(HTTP_RETRY_DELAY)
            return self.get_jmx(url)


    def get_beans(self, jmx_data):
        """
        Get JMX Beans as a Python Dictionary
        """
        return json.loads(jmx_data.decode('utf8'))['beans']


    def get_jmx_dict(self, jmx_url):
        """
        """
        return self.get_beans(self.get_jmx(jmx_url))

    def __set_properties(self):
        """
        Stub method
        """
        pass

    def refresh(self, jmx_dict):
        """
        Public method to set variables based on the latest JMX data
        """
        self.jmx_dict = self.get_jmx_dict(self.jmx_url)
        self.__set_properties(self.jmx_dict)



class NameNode(JmxHadoopHost):
    """
    HDFS NameNode host
    """
    def __init__(self, host, port=50070):
        """
        """
        self.description = 'HDFS NameNode'
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(NameNode, self).__init__(host, port)

        namenode_uri = '/hosts/' + self.fqdn + '/host_components/NAMENODE'
        self.ambari_url = self.ambari.url + namenode_uri

        self.state = self.__get_state(self.jmx_dict)
        self.safemode = self.__get_safemode(self.jmx_dict)
        self.livenodes = []
        self.__set_properties(self.jmx_dict)

        logging.debug('=> Initialization of %s complete.', self.description)


    def get_state(self):
        """
        Public method to query the NameNode if it is in ACTIVE or STANDBY mode.
        """
        self.jmx_dict = self.get_jmx_dict(self.jmx_url)
        self.state = self.__get_state(self.jmx_dict)
        return self.state


    def __get_state(self, jmx_dict):
        """
        Private method to search the JMX beans to determine if the NameNode
        is in ACTIVE or STANDBY mode.
        """
        for bean in jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=NameNode,name=NameNodeStatus':
                        logging.debug('NameNode: state = %s', bean['State'])
                        return bean['State']


    def get_safemode(self):
        """
        Public method to query the NameNode if it is in Safemode.
        """
        self.jmx_dict = self.get_jmx_dict(self.jmx_url)
        self.safemode = self.__get_safemode(self.jmx_dict)
        return self.safemode


    def __get_safemode(self, jmx_dict):
        """
        Private method to search the JMX beans to determine if the NameNode
        is in a 'Safemode' recovery state or operating properly.
        """
        for bean in jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=NameNode,name=NameNodeInfo':
                        if re.match('Safe mode is ON.', bean['Safemode']):
                            logging.debug("NameNode: safemode = True")
                            return True
                        else:
                            logging.debug("NameNode: safemode = False")
                            return False


    def get_livenodes(self):
        """
        Public method to get a list of LiveNodes from JMX
        """
        livenodes = []
        deadnodes = []
        for bean in self.jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=NameNode,name=NameNodeInfo':
                        for node in json.loads(bean['LiveNodes']).keys():
                            name = node.split('.')[0]
                            if re.match('In Service', json.loads(bean['LiveNodes'])[node]['adminState']):
                                livenodes.append(name)
                            else:
                                logging.warning('Found Dead Datanode: {0}'.format(name))
                                deadnodes.append(name)
        self.livenodes = sorted(livenodes)
        logging.debug('NameNode: livenodes = %s', self.livenodes)
        return self.livenodes


    def __set_properties(self, jmx_dict):
        """
        Private method to set variables based on JMX data
        """
        self.get_livenodes()
        for bean in jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=NameNode,name=NameNodeStatus':
                        self.security_enabled = True if bean['SecurityEnabled'] == 'true' else False
                        logging.debug('%s: security_enabled = %s', self.__class__.__name__, self.security_enabled)

                    if bean[key] == 'Hadoop:service=NameNode,name=StartupProgress':
                        self.startup_percent_complete = float(bean['PercentComplete'])
                        logging.debug('%s: startup_percent_complete = %f', self.__class__.__name__, self.startup_percent_complete)
                        self.loading_edits_percent_complete = float(bean['LoadingEditsPercentComplete'])
                        logging.debug('%s: loading_edits_percent_complete = %f', self.__class__.__name__, self.loading_edits_percent_complete)

                    if bean[key] == 'Hadoop:service=NameNode,name=FSNamesystem':
                        self.missing_blocks = int(bean['MissingBlocks'])
                        logging.debug('%s: missing_blocks = %i', self.__class__.__name__, self.missing_blocks)
                        self.missing_repl_one_blocks = int(bean['MissingReplOneBlocks'])
                        logging.debug('%s: missing_repl_one_blocks = %i', self.__class__.__name__, self.missing_repl_one_blocks)
                        self.expired_heartbeats = int(bean['ExpiredHeartbeats'])
                        logging.debug('%s: expired_heartbeats = %i', self.__class__.__name__, self.expired_heartbeats)
                        self.lock_queue_length = int(bean['LockQueueLength'])
                        logging.debug('%s: lock_queue_length = %i', self.__class__.__name__, self.lock_queue_length)
                        self.num_active_clients = int(bean['NumActiveClients'])
                        logging.debug('%s: num_active_clients = %i', self.__class__.__name__, self.num_active_clients)
                        self.pending_replication_blocks = int(bean['PendingReplicationBlocks'])
                        logging.debug('%s: pending_replication_blocks = %i', self.__class__.__name__, self.pending_replication_blocks)
                        self.under_replicated_blocks = int(bean['UnderReplicatedBlocks'])
                        logging.debug('%s: under_replicated_blocks = %i', self.__class__.__name__, self.under_replicated_blocks)
                        self.corrupt_blocks = int(bean['CorruptBlocks'])
                        logging.debug('%s: corrupt_blocks = %i', self.__class__.__name__, self.corrupt_blocks)
                        self.scheduled_replication_blocks = int(bean['ScheduledReplicationBlocks'])
                        logging.debug('%s: scheduled_replication_blocks = %i', self.__class__.__name__, self.scheduled_replication_blocks)
                        self.pending_deletion_blocks = int(bean['PendingDeletionBlocks'])
                        logging.debug('%s: pending_deletion_blocks = %i', self.__class__.__name__, self.pending_deletion_blocks)
                        self.excess_blocks = int(bean['ExcessBlocks'])
                        logging.debug('%s: excess_blocks = %i', self.__class__.__name__, self.excess_blocks)
                        self.postponed_misreplicated_blocks = int(bean['PostponedMisreplicatedBlocks'])
                        logging.debug('%s: postponed_misreplicated_blocks = %i', self.__class__.__name__, self.postponed_misreplicated_blocks)
                        self.pending_datanode_msg_count = int(bean['PendingDataNodeMessageCount'])
                        logging.debug('%s: pending_datanode_msg_count = %i', self.__class__.__name__, self.pending_datanode_msg_count)
                        self.stale_datanodes = int(bean['StaleDataNodes'])
                        logging.debug('%s: stale_datanodes = %i', self.__class__.__name__, self.stale_datanodes)

                    if bean[key] == 'Hadoop:service=NameNode,name=NameNodeInfo':
                        self.threads = int(bean['Threads'])
                        logging.debug('%s: threads = %i', self.__class__.__name__, self.threads)
                        self.finalized = True if bean['UpgradeFinalized'] == 'true' else False
                        logging.debug('%s: finalized = %s', self.__class__.__name__, self.finalized)
                        self.total_blocks = int(bean['TotalBlocks'])
                        logging.debug('%s: total_blocks = %i', self.__class__.__name__, self.total_blocks)
                        self.total_files = int(bean['TotalFiles'])
                        logging.debug('%s: total_files = %i', self.__class__.__name__, self.total_files)

    def is_healthy(self):
        """
        Public method to query the Health of the NameNode
        """
        return self.get_safemode()


class ZkFailoverController(HadoopHost):
    """
    HDFS ZooKeeper Failover Controller hosts which coexist on
    NameNode servers that support HA
    """
    def __init__(self, host, port=8019):
        """
        """
        self.description = "HDFS Failover Controller"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(ZkFailoverController, self).__init__(host, port)

        zkfc_uri = '/hosts/' + self.fqdn + '/host_components/ZKFC'
        self.ambari_url = self.ambari.url + zkfc_uri

        logging.debug('=> Initialization of %s complete.', self.description)



class DataNode(JmxHadoopHost):
    """
    HDFS DataNode host
    """
    def __init__(self, host, port=50075):
        """
        """
        self.description = "HDFS DataNode"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(DataNode, self).__init__(host, port)

        datanode_uri = '/hosts/' + self.fqdn + '/host_components/DATANODE'
        self.ambari_url = self.ambari.url + datanode_uri
        self.__set_properties(self.jmx_dict)

        logging.debug('=> Initialization of %s complete.', self.description)


    def __set_properties(self, jmx_dict):
        """
        Private method to set variables based on JMX data
        """
        for bean in jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=DataNode,name=FSDatasetState':
                        self.failed_volumes = int(bean['NumFailedVolumes'])
                        logging.debug('%s: failed_volumes = %i', self.__class__.__name__, self.failed_volumes)
                        self.capacity = int(bean['Capacity'])
                        logging.debug('%s: capacity = %i', self.__class__.__name__, self.capacity)
                        self.used = int(bean['DfsUsed'])
                        logging.debug('%s: used = %i', self.__class__.__name__, self.used)
                        self.remaining = int(bean['Remaining'])
                        logging.debug('%s: remaining = %i', self.__class__.__name__, self.remaining)
                        self.lost_capacity = int(bean['EstimatedCapacityLostTotal'])
                        logging.debug('%s: lost_capacity = %i', self.__class__.__name__, self.lost_capacity)

                    if bean[key] == 'Hadoop:service=DataNode,name=DataNodeInfo':
                        self.namenodes = []
                        for namenode in json.loads(bean['NamenodeAddresses']).keys():
                            self.namenodes.append(namenode.split('.')[0])

                    if bean[key] == 'Hadoop:service=DataNode,name=JvmMetrics':
                        self.blocked_threads = int(bean['ThreadsBlocked'])
                        logging.debug('%s: blocked_threads = %i', self.__class__.__name__, self.blocked_threads)


    def is_healthy(self):
        """
        Public method to query the Health of the DataNode
        """
        self.refresh()
        return self.failed_volumes == 0 and len(self.namenodes) > 1


class JournalNode(JmxHadoopHost):
    """
    HDFS Journal Node
    """
    def __init__(self, host, port=8480):
        """
        """
        self.description = "HDFS Journal"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(JournalNode, self).__init__(host, port)

        journalnode_uri = '/hosts/' + self.fqdn + '/host_components/JOURNALNODE'
        self.ambari_url = self.ambari.url + journalnode_uri

        logging.debug('=> Initialization of %s complete.', self.description)


class NfsGateway(JmxHadoopHost):
    """
    NFS Gateway for HDFS
    """
    def __init__(self, host, port=50079):
        """
        """
        self.description = "NFS Gateway"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(NfsGateway, self).__init__(host, port)

        nfsgateway_uri = '/hosts/' + self.fqdn + '/host_components/NFS_GATEWAY'
        self.ambari_url = self.ambari.url + nfsgateway_uri

        logging.debug('=> Initialization of %s complete.', self.description)


class ResourceManager(JmxHadoopHost):
    """
    YARN ResourceManager
    """
    def __init__(self, host, port=8088):
        """
        """
        self.description = "YARN ResourceManager"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(ResourceManager, self).__init__(host, port)


        resourcemanager_uri = '/hosts/' + self.fqdn + '/host_components/RESOURCEMANAGER'
        self.ambari_url = self.ambari.url + resourcemanager_uri
        self.__set_properties(self.jmx_dict)
        self.get_state()
        self.get_livenodes()

        logging.debug('=> Initialization of %s complete.', self.description)

    def get_state(self):
        """
        Public method to query the ResourceManager if it is in ACTIVE or STANDBY mode.
        """
        url = 'http://{0}:{1}/cluster'.format(self.fqdn, self.port)
        try:
            req = requests.get(url, allow_redirects=False)
            if req.status_code == 302 or req.status_code == 307:
                self.state = 'standby'
                return self.state
            elif req.status_code == 200:
                self.state = 'active'
                return self.state
            else:
                logging.error('HTTP GET to %s returned contents: %s', url, req.content)
                time.sleep(HTTP_RETRY_DELAY)
                return self.get_state()
        except Exception as exception:
            logging.warning('%s: HTML data not returned from %s, Retrying...', exception.__class__.__name__, url)
            time.sleep(HTTP_RETRY_DELAY)
            return self.get_state()


    def get_livenodes(self):
        """
        Public method to get a list of LiveNodes from JMX
        """
        livenodes = []
        for bean in self.jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=ResourceManager,name=RMNMInfo':
                        for node in json.loads(bean['LiveNodeManagers']):
                            name = node['HostName'].split('.')[0]
                            livenodes.append(name)
        self.livenodes = sorted(livenodes)
        logging.debug('%s: livenodes = %s', self.__class__.__name__, self.livenodes)
        return self.livenodes

    def __set_properties(self, jmx_dict):
        """
        Private method to set variables based on JMX data
        """
        for bean in jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=ResourceManager,name=MetricsSystem,sub=Stats':
                        self.num_active_sources = int(bean['NumActiveSources'])
                        logging.debug('%s: num_active_sources = %i', self.__class__.__name__, self.num_active_sources)
                        self.num_all_sources = int(bean['NumAllSources'])
                        logging.debug('%s: num_all_sources = %i', self.__class__.__name__, self.num_all_sources)
                        self.num_active_sinks = int(bean['NumActiveSinks'])
                        logging.debug('%s: num_active_sinks = %i', self.__class__.__name__, self.num_active_sinks)

                    if bean[key] == 'Hadoop:service=ResourceManager,name=ClusterMetrics':
                        self.num_active_nodemanagers = int(bean['NumActiveNMs'])
                        logging.debug('%s: num_active_nodemanagers = %i', self.__class__.__name__, self.num_active_nodemanagers)
                        self.num_decommissioned_nodemanagers = int(bean['NumDecommissionedNMs'])
                        logging.debug('%s: num_decommissioned_nodemanagers = %i', self.__class__.__name__, self.num_decommissioned_nodemanagers)
                        self.num_lost_nodemanagers = int(bean['NumLostNMs'])
                        logging.debug('%s: num_lost_nodemanagers = %i', self.__class__.__name__, self.num_lost_nodemanagers)
                        self.num_unhealthy_nodemanagers = int(bean['NumUnhealthyNMs'])
                        logging.debug('%s: num_unhealthy_nodemanagers = %i', self.__class__.__name__, self.num_unhealthy_nodemanagers)
                        self.num_rebooted_nodemanagers = int(bean['NumRebootedNMs'])
                        logging.debug('%s: num_rebooted_nodemanagers = %i', self.__class__.__name__, self.num_rebooted_nodemanagers)


    def is_healthy(self):
        """
        Public method to query the Health of the ResourceManager
        """
        self.refresh()
        return self.num_unhealthy_nodemanagers == 0 and self.num_lost_nodemanagers == 0



class NodeManager(JmxHadoopHost):
    """
    YARN NodeManger
    """
    def __init__(self, host, port=8042):
        """
        """
        self.description = "YARN NodeManger"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(NodeManager, self).__init__(host, port)

        nodemanager_uri = '/hosts/' + self.fqdn + '/host_components/NODEMANAGER'
        self.ambari_url = self.ambari.url + nodemanager_uri
        self.__set_properties(self.jmx_dict)

        logging.debug('=> Initialization of %s complete.', self.description)

    def __set_properties(self, jmx_dict):
        """
        Private method to set variables based on JMX data
        """
        for bean in jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=NodeManager,name=NodeManagerMetrics':
                        self.containers_initing = int(bean['ContainersIniting'])
                        logging.debug('%s: containers_initing = %i', self.__class__.__name__, self.containers_initing)
                        self.containers_running = int(bean['ContainersRunning'])
                        logging.debug('%s: containers_running = %i', self.__class__.__name__, self.containers_running)
                        self.bad_local_dirs = int(bean['BadLocalDirs'])
                        logging.debug('%s: bad_local_dirs = %i', self.__class__.__name__, self.bad_local_dirs)
                        self.bad_log_dirs = int(bean['BadLogDirs'])
                        logging.debug('%s: bad_log_dirs = %i', self.__class__.__name__, self.bad_log_dirs)

                    if bean[key] == 'Hadoop:service=NodeManager,name=ShuffleMetrics':
                        self.shuffle_outputs_failed = int(bean['ShuffleOutputsFailed'])
                        logging.debug('%s: shuffle_outputs_failed = %i', self.__class__.__name__, self.shuffle_outputs_failed)
                        self.shuffle_connections = int(bean['ShuffleConnections'])
                        logging.debug('%s: shuffle_connections = %i', self.__class__.__name__, self.shuffle_connections)

                    if bean[key] == 'Hadoop:service=NodeManager,name=MetricsSystem,sub=Stats':
                        self.num_active_sources = int(bean['NumActiveSources'])
                        logging.debug('%s: num_active_sources = %i', self.__class__.__name__, self.num_active_sources)
                        self.num_all_sources = int(bean['NumAllSources'])
                        logging.debug('%s: num_all_sources = %i', self.__class__.__name__, self.num_all_sources)
                        self.num_active_sinks = int(bean['NumActiveSinks'])
                        logging.debug('%s: num_active_sinks = %i', self.__class__.__name__, self.num_active_sinks)
                        self.sink_timeline_dropped = int(bean['Sink_timelineDropped'])
                        logging.debug('%s: sink_timeline_dropped = %i', self.__class__.__name__, self.sink_timeline_dropped)
                        self.sink_timeline_q_size = int(bean['Sink_timelineQsize'])
                        logging.debug('%s: sink_timeline_q_size = %i', self.__class__.__name__, self.sink_timeline_q_size)


class AppTimeline(JmxHadoopHost):
    """
    App Timeline server for YARN
    """
    def __init__(self, host, port=8188):
        """
        """
        self.description = "App Timeline Server"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(AppTimeline, self).__init__(host, port)

        apptimeline_uri = '/hosts/' + self.fqdn + '/host_components/APP_TIMELINE_SERVER'
        self.ambari_url = self.ambari.url + apptimeline_uri

        logging.debug('=> Initialization of %s complete.', self.description)


class HbaseMaster(JmxHadoopHost):
    """
    HBase Master server
    """
    def __init__(self, host, port=16010):
        """
        """
        self.description = 'HBase Master'
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(HbaseMaster, self).__init__(host, port)

        hbasemaster_uri = '/hosts/' + self.fqdn + '/host_components/HBASE_MASTER'
        self.ambari_url = self.ambari.url + hbasemaster_uri

        self.state = self.__get_state(self.jmx_dict)
        self.get_livenodes()
        self.__set_properties(self.jmx_dict)

        logging.debug('=> Initialization of %s complete.', self.description)


    def get_state(self):
        """
        Public method to query the HBase Master if it is in ACTIVE or STANDBY mode.
        """
        self.jmx_dict = self.get_jmx_dict(self.jmx_url)
        self.state = self.__get_state(self.jmx_dict)
        return self.state


    def __get_state(self, jmx_dict):
        """
        Private method to search the JMX beans to determine if the HBase Master
        is in ACTIVE or STANDBY mode.
        """
        for bean in jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=HBase,name=Master,sub=Server':
                        if bean['tag.isActiveMaster'] == "true":
                            state = 'active'
                        else:
                            state = 'standby'
                        logging.debug('%s: state = %s', self.__class__.__name__, state)
                        return state

    def get_livenodes(self):
        """
        Public method to get a list of LiveNodes from JMX
        """
        livenodes = []
        deadnodes = []
        for bean in self.jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=HBase,name=Master,sub=Server':
                        for node in bean['tag.liveRegionServers'].split(';'):
                            name = node.split('.')[0]
                            livenodes.append(name)
                        for node in bean['tag.deadRegionServers'].split(';'):
                            name = node.split('.')[0]
                            deadnodes.append(name)
        self.livenodes = sorted(livenodes)
        self.deadnodes = sorted(deadnodes)
        logging.debug('%s: livenodes = %s', self.__class__.__name__, self.livenodes)
        return self.livenodes


    def __set_properties(self, jmx_dict):
        """
        Private method to set variables based on JMX data
        """
        for bean in jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=HBase,name=Master,sub=Balancer':
                        self.balancer_ops = int(bean['BalancerCluster_num_ops'])
                        logging.debug('%s: balancer_ops = %i', self.__class__.__name__, self.balancer_ops)
                        self.balancer_min = int(bean['BalancerCluster_min'])
                        logging.debug('%s: balancer_min = %i', self.__class__.__name__, self.balancer_min)
                        self.balancer_max = int(bean['BalancerCluster_max'])
                        logging.debug('%s: balancer_max = %i', self.__class__.__name__, self.balancer_max)
                        self.balancer_mean = float(bean['BalancerCluster_mean'])
                        logging.debug('%s: balancer_mean = %f', self.__class__.__name__, self.balancer_mean)

                    if bean[key] == 'Hadoop:service=HBase,name=Master,sub=AssignmentManger':
                        self.rit_oldest_age = int(bean['ritOldestAge'])
                        logging.debug('%s: rit_oldest_age = %i', self.__class__.__name__, self.rit_oldest_age)
                        self.rit_over_threshold = int(bean['ritCountOverThreshold'])
                        logging.debug('%s: rit_over_threshold = %i', self.__class__.__name__, self.rit_over_threshold)
                        self.rit_count = int(bean['ritCount'])
                        logging.debug('%s: rit_count = %i', self.__class__.__name__, self.rit_count)
                        self.assign_ops = int(bean['Assign_num_ops'])
                        logging.debug('%s: assign_ops = %i', self.__class__.__name__, self.assign_ops)
                        self.assign_min = int(bean['Assign_min'])
                        logging.debug('%s: assign_min = %i', self.__class__.__name__, self.assign_min)
                        self.assign_max = int(bean['Assign_max'])
                        logging.debug('%s: assign_max = %i', self.__class__.__name__, self.assign_max)
                        self.assign_mean = float(bean['Assign_mean'])
                        logging.debug('%s: assign_mean = %f', self.__class__.__name__, self.assign_mean)

                    if bean[key] == 'Hadoop:service=HBase,name=Master,sub=Server':
                        self.average_load = float(bean['averageLoad'])
                        logging.debug('%s: average_load = %f', self.__class__.__name__, self.average_load)
                        self.num_regionservers = int(bean['numRegionServers'])
                        logging.debug('%s: num_regionservers = %i', self.__class__.__name__, self.num_regionservers)
                        self.num_dead_regionservers = int(bean['numDeadRegionServers'])
                        logging.debug('%s: num_dead_regionservers = %i', self.__class__.__name__, self.num_dead_regionservers)


    def is_healthy(self):
        """
        Public method to query the Health of the HBase Master
        """
        self.refresh()
        return self.num_dead_regionservers == 0



class RegionServer(JmxHadoopHost):
    """
    HBase Region server
    """
    def __init__(self, host, port=16030):
        """
        """
        self.description = "HBase Region Server"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(RegionServer, self).__init__(host, port)

        regionserver_uri = '/hosts/' + self.fqdn + '/host_components/HBASE_REGIONSERVER'
        self.ambari_url = self.ambari.url + regionserver_uri

        logging.debug('=> Initialization of %s complete.', self.description)


class PhoenixQueryServer(HadoopHost):
    """
    Phoenix Query server for HBase
    """
    def __init__(self, host, port=8765):
        """
        """
        self.description = "Phoenix Query Server"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(PhoenixQueryServer, self).__init__(host, port)

        phoenixqueryserver_uri = '/hosts/' + self.fqdn + '/host_components/PHOENIX_QUERY_SERVER'
        self.ambari_url = self.ambari.url + phoenixqueryserver_uri

        logging.debug('=> Initialization of %s complete.', self.description)



class JobHistory(JmxHadoopHost):
    """
    """
    def __init__(self, host, port=19888):
        """
        """
        self.description = "MapReduce2 Job History"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(JobHistory, self).__init__(host, port)

        jobhistory_uri = '/hosts/' + self.fqdn + '/host_components/HISTORYSERVER'
        self.ambari_url = self.ambari.url + jobhistory_uri

        logging.debug('=> Initialization of %s complete.', self.description)



class Oozie(HadoopHost):
    """
    """
    def __init__(self, host, port=11000):
        """
        """
        self.description = "Oozie Server"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(Oozie, self).__init__(host, port)

        oozie_uri = '/hosts/' + self.fqdn + '/host_components/OOZIE_SERVER'
        self.ambari_url = self.ambari.url + oozie_uri

        logging.debug('=> Initialization of %s complete.', self.description)


class ZooKeeper(HadoopHost):
    """
    """
    def __init__(self, host, port=2181):
        """
        """
        self.description = "ZooKeeper"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(ZooKeeper, self).__init__(host, port)

        zookeeper_uri = '/hosts/' + self.fqdn + '/host_components/ZOOKEEPER_SERVER'
        self.ambari_url = self.ambari.url + zookeeper_uri

        logging.debug('=> Initialization of %s complete.', self.description)



class SparkHistory(HadoopHost):
    """
    """
    def __init__(self, host, port=18080):
        """
        """
        self.description = "Spark History"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(SparkHistory, self).__init__(host, port)

        sparkhistory_uri = '/hosts/' + self.fqdn + '/host_components/SPARK_JOBHISTORYSERVER'
        self.ambari_url = self.ambari.url + sparkhistory_uri

        logging.debug('=> Initialization of %s complete.', self.description)


class SparkThrift(HadoopHost):
    """
    """
    def __init__(self, host, port=4040):
        """
        """
        self.description = "Spark Thrift"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(SparkThrift, self).__init__(host, port)

        sparkhistory_uri = '/hosts/' + self.fqdn + '/host_components/SPARK_THRIFTSERVER'
        self.ambari_url = self.ambari.url + sparkhistory_uri

        logging.debug('=> Initialization of %s complete.', self.description)



class Kafka(HadoopHost):
    """
    """
    def __init__(self, host, port=6667):
        """
        """
        self.description = "Kafka Broker"
        logging.debug('=> Initializing %s: %s...', self.description, host.fqdn)
        super(Kafka, self).__init__(host, port)

        sparkhistory_uri = '/hosts/' + self.fqdn + '/host_components/KAFKA_BROKER'
        self.ambari_url = self.ambari.url + sparkhistory_uri

        logging.debug('=> Initialization of %s complete.', self.description)


@click.command()
@click.option('--hostname', prompt='Enter Ambari Hostname', help='The hostname of the Ambari server.')
@click.option('--username', prompt='Enter Admin Username', help='A user account that has admin privileges.')
@click.option('--password', prompt='Enter Admin Password', help='The password for the admin user.')
@click.option('--cluster', default='pdxlab', help='')
@click.option('--domain', default='lab.pdx.org', help='')
@click.option('--port', default=8080, help='')
@click.argument('service')
def init_script(hostname, username, password, cluster, domain, port, service):
    """
    """
    name = hostname.split('.')[0]

    logging.debug('===> Step 1. Connect to Ambari server and Discover Ambari-managed Hosts.')
    ambari = Ambari(name, username, password, cluster, domain, port)

    logging.debug('===> Step 2. Select the proper subroutine(s) to execute.')

    hdp_svc = service.upper()

    if hdp_svc in ('HDFS', 'ALL'):
        restart_hdfs(ambari)

    if hdp_svc in ('YARN', 'ALL'):
        restart_yarn(ambari)

    if hdp_svc in ('HBASE', 'ALL'):
        restart_hbase(ambari)

    if hdp_svc in ('MR2', 'MAPREDUCE2', 'ALL'):
        restart_mr2(ambari)

    if hdp_svc in ('TEZ', 'ALL'):
        restart_tez(ambari)

    if hdp_svc in ('HIVE', 'ALL'):
        restart_hive(ambari)

    if hdp_svc in ('PIG', 'ALL'):
        restart_pig(ambari)

    if hdp_svc in ('OOZIE', 'ALL'):
        restart_oozie(ambari)

    if hdp_svc in ('ZOOKEEPER', 'ALL'):
        restart_zookeeper(ambari)

    if hdp_svc in ('SPARK', 'ALL'):
        restart_spark(ambari)

    if hdp_svc in ('KAFKA', 'ALL'):
        restart_kafka(ambari)



def fast_restart(node):
    """
    Implements a health-based restart policy rather just waiting a specified
    amount of time. This procedure depends heavily on the is_healthy() method
    being properly implemented for the service. If the service only uses the
    default TCP healthcheck, this is probably not how it should be restarted.
    """
    logging.info('Restarting %s on %s...', node.description, node.fqdn)
    node.stop()

    while node.tcp_port_open():
        time.sleep(HTTP_RETRY_DELAY)

    node.start()

    while node.tcp_port_closed():
        time.sleep(HTTP_RETRY_DELAY)

    count = 0
    while not node.is_healthy():
        count += 1
        if count < MAX_RETRY_BEFORE_RESTART:
            logging.info('Waiting for %s to recover on %s...', node.description, node.fqdn)
            sleep(HTTP_RETRY_DELAY)
        else:
            node.start()


def restart_hdfs(ambari):
    """
    Function which acts as a manifest to restart HDFS services
    """
    service_name = 'HDFS'
    namenodes = []
    zkfcs = []
    journalnodes = []
    datanodes = []
    nfs_gateways = []

    logging.debug('==> Beginning %s restart...', service_name)

    for host in ambari.hosts:
        if host.namenode:
            if host.zkfc:
                namenodes.append(NameNode(host))
                zkfcs.append(ZkFailoverController(host))
            else:
                logging.error('NameNode on %s does not have a Failover Controller!', host.fqdn)
                raise Exception('This is an unknown configuration state. Exiting...')

        if host.journalnode:
            journalnodes.append(JournalNode(host))

        if host.datanode:
            datanodes.append(DataNode(host))

        if host.nfs_gateway:
            nfs_gateways.append(NfsGateway(host))

    # Restart ONLY the standby NameNode(s)
    for node in namenodes:
        if node.state == 'standby':

            # Do not proceed until the NameNode exits SafeMode
            while node.get_safemode():
                logging.warning('%s on %s is in SafeMode. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)

            logging.info('Restarting %s on %s...', node.description, node.fqdn)
            node.stop()
            time.sleep(NAMENODE_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(NAMENODE_RESTART_DELAY)

            # Do not proceed until the NameNode exits SafeMode
            while node.get_safemode():
                logging.warning('%s on %s is in SafeMode. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)

            # Do not proceed unless this NameNode is a Standby
            while node.get_state() != 'standby':
                logging.warning('%s on %s is not a Standby. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)

            for zkfc in zkfcs:
                if zkfc.fqdn == node.fqdn:
                    fast_restart(zkfc)

            # Do not proceed until the NameNode exits SafeMode
            while node.get_safemode():
                logging.warning('%s on %s is in SafeMode. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)

            # Do not proceed unless this NameNode is a Standby
            while node.get_state() != 'standby':
                logging.warning('%s on %s is not a Standby. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)


    # Restart ONLY the Active NameNode
    for node in namenodes:
        if node.state == 'active':

            # Do not proceed until the NameNode exits SafeMode
            while node.get_safemode():
                logging.warning('%s on %s is in SafeMode. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)

            logging.info('Restarting %s on %s...', node.description, node.fqdn)
            node.stop()
            time.sleep(NAMENODE_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(NAMENODE_RESTART_DELAY)

            # Do not proceed until the NameNode exits SafeMode
            while node.get_safemode():
                logging.warning('%s on %s is in SafeMode. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)

            # Do not proceed unless this NameNode is a Standby
            while node.get_state() != 'standby':
                logging.warning('%s on %s is not a Standby. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)

            for zkfc in zkfcs:
                if zkfc.fqdn == node.fqdn:
                    logging.info('Restarting %s on %s...', zkfc.description, zkfc.fqdn)
                    zkfc.stop()
                    time.sleep(NAMENODE_RESTART_DELAY)
                    while zkfc.tcp_port_closed():
                        zkfc.start()
                        time.sleep(NAMENODE_RESTART_DELAY)

            # Do not proceed until the NameNode exits SafeMode
            while node.get_safemode():
                logging.warning('%s on %s is in SafeMode. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)

            # Do not proceed unless this NameNode is a Standby
            while node.get_state() != 'standby':
                logging.warning('%s on %s is not a Standby. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)


    # Find the new Active NameNode
    for node in namenodes:
        print(node.get_state())
        if node.get_state() == 'active':
            try:
                livenodes = node.get_livenodes()
            except Exception as exception:
                logging.error('%s: There are no active NameNodes!', exception)
                raise Exception('Exiting...')


    # Restart the HDFS JournalNodes
    for node in journalnodes:
        logging.info('Restarting %s on %s...', node.description, node.fqdn)
        node.stop()
        time.sleep(JOURNALNODE_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(JOURNALNODE_RESTART_DELAY)


    # Restart the HDFS Datanodes
    for node in datanodes:
        if node.name in livenodes:
            logging.info('Restarting %s on %s...', node.description, node.fqdn)
            node.stop()
            time.sleep(DATANODE_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(DATANODE_RESTART_DELAY)
        else:
            logging.error('DataNode on %s is not in the LiveNodes list on the active NameNode', node.fqdn)
            raise Exception('This is an unknown condition. Exiting...')

    for node in nfs_gateways:
        logging.info('Restarting %s on %s...', node.description, node.fqdn)
        node.stop()
        time.sleep(DATANODE_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(DATANODE_RESTART_DELAY)

   # Refresh the client configs
    for host in ambari.hosts:
        logging.info('Refreshing %s client configs on %s...', service_name, host.fqdn)
        HadoopService(host, service_name.upper()).refresh()


def restart_yarn(ambari):
    """
    Function which acts as a manifest to restart YARN services
    """
    service_name = 'YARN'
    resourcemanagers = []
    nodemanagers = []
    apptimelines = []

    logging.debug('==> Beginning %s restart...', service_name)

    for host in ambari.hosts:
        if host.resourcemanager:
            resourcemanagers.append(ResourceManager(host))
        if host.nodemanager:
            nodemanagers.append(NodeManager(host))
        if host.apptimeline:
            apptimelines.append(AppTimeline(host))

    # First restart the Standby Resource Manager(s)
    for node in resourcemanagers:

        if node.state == 'standby':

            logging.info('Restarting %s on %s...', node.description, node.fqdn)
            node.stop()
            time.sleep(RESOURCEMANAGER_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(RESOURCEMANAGER_RESTART_DELAY)

            # Do not proceed unless this ResourceManager is a Standby
            while node.get_state() != 'standby':
                logging.warning('%s on %s is not a Standby. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)


    # Now restart the Active Resource Manager
    for node in resourcemanagers:

        if node.state == 'active':

            logging.info('Restarting %s on %s...', node.description, node.fqdn)
            node.stop()
            time.sleep(RESOURCEMANAGER_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(RESOURCEMANAGER_RESTART_DELAY)

            # Do not proceed unless this ResourceManager is a Standby
            while node.get_state() != 'standby':
                logging.warning('%s on %s is not a Standby. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)

    for node in nodemanagers:
        logging.info('Restarting %s on %s...', node.description, node.fqdn)
        node.stop()
        time.sleep(NODEMANAGER_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(NODEMANAGER_RESTART_DELAY)

    for node in apptimelines:
        fast_retsart(node)

   # Refresh the client configs
    for host in ambari.hosts:
        logging.info('Refreshing %s client configs on %s...', service_name, host.fqdn)
        HadoopService(host, service_name.upper()).refresh()


def restart_hbase(ambari):
    """
    Function which acts as a manifest to restart HBase services
    """
    service_name = 'HBase'
    hbasemasters = []
    regionservers = []
    queryservers = []

    logging.debug('==> Beginning %s restart...', service_name)

    for host in ambari.hosts:
        if host.hbasemaster:
            hbasemasters.append(HbaseMaster(host))

        if host.regionserver:
            regionservers.append(RegionServer(host))

        if host.phoenix:
            queryservers.append(PhoenixQueryServer(host))


    # First restart the Standby HBase Master(s)
    for node in hbasemasters:

        if node.state == 'standby':

            logging.info('Restarting %s on %s...', node.description, node.fqdn)
            node.stop()
            time.sleep(HBASEMASTER_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(HBASEMASTER_RESTART_DELAY)

            # Do not proceed unless this HBase Master is a Standby
            while node.get_state() != 'standby':
                logging.warning('%s on %s is not a Standby. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)


    # Now restart the Active HBase Master
    for node in hbasemasters:

        if node.state == 'active':

            logging.info('Restarting %s on %s...', node.description, node.fqdn)
            node.stop()
            time.sleep(HBASEMASTER_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(HBASEMASTER_RESTART_DELAY)

            # Do not proceed unless this HBase Master is a Standby
            while node.get_state() != 'standby':
                logging.warning('%s on %s is not a Standby. Retrying...', node.description, node.fqdn)
                time.sleep(SAFEMODE_RETRY_DELAY)


    for node in regionservers:
        logging.info('Restarting %s on %s...', node.description, node.fqdn)
        node.stop()
        time.sleep(REGIONSERVER_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(REGIONSERVER_RESTART_DELAY)

    for node in queryservers:
        logging.info('Restarting %s on %s...', node.description, node.fqdn)
        node.stop()
        time.sleep(PHXQUERYSERVER_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(PHXQUERYSERVER_RESTART_DELAY)

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info('Refreshing %s client configs on %s...', service_name, host.fqdn)
        HadoopService(host, service_name.upper()).refresh()


def restart_mr2(ambari):
    """
    Function which acts as a manifest to restart MapReduce2 services
    """
    service_name = 'MapReduce2'
    jobhistories = []

    for host in ambari.hosts:
        if host.jobhistory:
            jobhistories.append(JobHistory(host))

    for node in jobhistories:
        fast_restart(node)

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info('Refreshing %s client configs on %s...', service_name, host.fqdn)
        HadoopService(host, service_name.upper()).refresh()


def restart_tez(ambari):
    """
    Function which acts as a manifest to restart Tez services
    """
    service_name = 'Tez'

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info('Refreshing %s client configs on %s...', service_name, host.fqdn)
        HadoopService(host, service_name.upper()).refresh()


def restart_hive(ambari):
    """
    Function which acts as a manifest to restart Hive services
    """
    pass


def restart_pig(ambari):
    """
    Function which acts as a manifest to restart Pig services
    """
    service_name = 'Pig'

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info('Refreshing %s client configs on %s...', service_name, host.fqdn)
        HadoopService(host, service_name.upper()).restart()


def restart_oozie(ambari):
    """
    Function which acts as a manifest to restart Oozie services
    """
    service_name = 'Oozie'
    oozies = []

    for host in ambari.hosts:
        if host.oozie:
            oozies.append(Oozie(host))

    for node in oozies:
        fast_restart(node)

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info('Refreshing %s client configs on %s...', service_name, host.fqdn)
        HadoopService(host, service_name.upper()).refresh()


def restart_zookeeper(ambari):
    """
    Function which acts as a manifest to restart ZooKeeper services
    """
    service_name = 'ZooKeeper'
    zookeepers = []

    for host in ambari.hosts:

        if host.zookeeper:
            zookeepers.append(ZooKeeper(host))

    for node in zookeepers:
        logging.info('Restarting %s on %s...', node.description, node.fqdn)
        node.stop()
        time.sleep(ZOOKEEPER_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(ZOOKEEPER_RESTART_DELAY)

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info('Refreshing %s client configs on %s...', service_name, host.fqdn)
        HadoopService(host, service_name.upper()).refresh()


def restart_spark(ambari):
    """
    Function which acts as a manifest to restart Spark services
    """
    service_name = 'Spark'
    sparkhistories = []
    sparkthrifts = []

    for host in ambari.hosts:
        if host.sparkhistory:
            sparkhistories.append(SparkHistory(host))
        if host.sparkthrift:
            sparkhistories.append(SparkThrift(host))

    for node in sparkhistories:
        fast_restart(node)

    for node in sparkthrifts:
        logging.info('Restarting %s on %s...', node.description, node.fqdn)
        node.stop()
        time.sleep(SPARKTHRIFT_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(SPARKTHRIFT_RESTART_DELAY)

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info('Refreshing %s client configs on %s...', service_name, host.fqdn)
        HadoopService(host, service_name.upper()).refresh()


def restart_kafka(ambari):
    """
    Function which acts as a manifest to restart Kafka services
    """
    service_name = 'Kafka'
    kafkas = []

    for host in ambari.hosts:
        if host.kafka:
            kafkas.append(Kafka(host))

    for node in kafkas:
        logging.info('Restarting %s on %s...', node.description, node.fqdn)
        node.stop()
        time.sleep(KAFKA_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(KAFKA_RESTART_DELAY)



if __name__ == '__main__':

    init_script()


