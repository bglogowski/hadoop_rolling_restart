#!/usr/bin/env python3.5

import click
import re
import json
import time
import logging
import requests
import socket


# Set constants for various aspects of the restarts

HTTP_RETRY_DELAY=5
NAMENODE_RESTART_DELAY=120
SAFEMODE_RETRY_DELAY=30
DATANODE_RESTART_DELAY=120
JOURNALNODE_RESTART_DELAY=120
RESOURCEMANAGER_RESTART_DELAY=120
NODEMANAGER_RESTART_DELAY=120
HBASEMASTER_RESTART_DELAY=120
REGIONSERVER_RESTART_DELAY=120
OOZIE_RESTART_DELAY=120
ZOOKEEPER_RESTART_DELAY=120
SPARKHISTORY_RESTART_DELAY=120
KAFKA_RESTART_DELAY=900

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
        'SPARK_CLIENT'
    ]
}


# Configure the syslog settings

FORMAT="%(asctime)s %(levelname)s %(message)s"

logging.basicConfig(format=FORMAT, level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class Ambari(object):
    """
    Ambari Server Object
    """
    def __init__(self, name, username, password, cluster, domain, port):
        logging.debug("Initializing Ambari server: {0}...".format(name))
        self.url = 'http://' + name + '.' + domain + ':' + str(port) + '/api/v1/clusters/' + cluster
        self.name = name
        self.username = username
        self.password = password
        self.domain = domain
        self.cluster = cluster
        self.port = port
        self.headers = { 'X-Requested-By' : 'hadoop.py' }
        self.__set_hosts()
        logging.debug("Initialization for Ambari for {0} complete.".format(name))

    def get_items(self, uri):
        try:
            r = requests.get(self.url + uri,
                             auth=(self.username, self.password),
                             headers=self.headers
                            ).content
            return json.loads(r.decode('utf8'))['items']
        except:
            logging.warning('JSON Data for {0} not returned from {1}, Retrying...'.format(self.name, self.url + uri))
            time.sleep(HTTP_RETRY_DELAY)
            return self.get_items(uri)

    def execute(self, url, payload):
        logging.debug('Attempting HTTP PUT to {0} with payload: {2} ...'.format(url, self.headers, payload))
        try:
            r = requests.put(url,
                             auth=(self.username, self.password),
                             headers=self.headers,
                             data=json.dumps(payload)
                            )
            if r.status_code == 202:
                return 'OK'
            else:
                logging.error('HTTP PUT to {0} returned {1} with contents: {2}'.format(url, r.status_code, json.loads(r.content.decode('utf8'))))
                time.sleep(HTTP_RETRY_DELAY)
                return self.execute(url, payload)
        except:
            logging.warning('JSON Data for {0} not returned from {1}, Retrying...'.format(self.name, url))
            time.sleep(HTTP_RETRY_DELAY)
            return self.execute(url, payload)


    def queue(self, url, payload):
        logging.debug('Attempting HTTP POST to {0} with payload: {2} ...'.format(url, self.headers, payload))
        try:
            r = requests.post(url,
                             auth=(self.username, self.password),
                             headers=self.headers,
                             data=json.dumps(payload)
                            )
            if r.status_code == 202:
                return 'OK'
            else:
                logging.error('HTTP POST to {0} returned {1} with contents: {2}'.format(url, r.status_code, json.loads(r.content.decode('utf8'))))
                time.sleep(HTTP_RETRY_DELAY)
                return self.queue(url, payload)
        except:
            logging.warning('JSON Data for {0} not returned from {1}, Retrying...'.format(self.name, url))
            time.sleep(HTTP_RETRY_DELAY)
            return self.queue(url, payload)




    def __set_hosts(self):
        hosts = []
        uri = '/hosts'
        for item in self.get_items(uri):
            name = item['Hosts']['host_name'].split('.')[0]
            logging.debug("=> Found Host: {0}".format(name))
            domain = '.'.join(item['Hosts']['host_name'].split('.')[1:])
            hosts.append(Host(name, domain, self))
        self.hosts = hosts


class Host(object):
    """
    Host managed by Ambari
    """
    def __init__(self, name, domain, ambari):
        """
        """
        logging.debug("Initializing Host: {0}...".format(name))

        self.name = name
        self.domain = domain
        self.fqdn = name + '.' + domain
        self.cluster = ambari.cluster
        self.ambari = ambari

        self.uri = '/hosts/' + self.fqdn
        self.url = self.ambari.url + self.uri

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
        self.zookeeper = False
        self.oozie = False
        self.sparkhistory = False
        self.kafka = False

        self.services = []
        self.__set_services()

        logging.debug("Initialization for Host {0} complete.".format(name))


    def __set_services(self):
        """
        """
        uri = self.uri + '/host_components'
        for component in self.ambari.get_items(uri):
            service= component['HostRoles']['component_name']
            self.services.append(service)

            # Anything can run on any server, check them all!


            # HDFS services

            if service == 'NAMENODE':
                logging.debug("Found HDFS NameNode on Host {0}...".format(self.name))
                self.namenode = True

            if service == 'JOURNALNODE':
                logging.debug("Found HDFS JournalNode on Host {0}...".format(self.name))
                self.journalnode = True

            if service == 'DATANODE':
                logging.debug("Found HDFS DataNode on Host {0}...".format(self.name))
                self.datanode = True

            if service == 'ZKFC':
                logging.debug("Found HDFS ZkFailoverController on Host {0}...".format(self.name))
                self.zkfc = True

            if service == 'NFS_GATEWAY':
                logging.debug("Found HDFS NFS Gateway on Host {0}...".format(self.name))
                self.nfs_gateway = True


            # HBase services

            if service == 'HBASE_MASTER':
                logging.debug("Found HBase Master on Host {0}...".format(self.name))
                self.hbasemaster = True

            if service == 'HBASE_REGIONSERVER':
                logging.debug("Found HBase Region Server on Host {0}...".format(self.name))
                self.regionserver = True

            if service == 'PHOENIX_QUERY_SERVER':
                logging.debug("Found HBase Phoenix Query Server on Host {0}...".format(self.name))
                self.phoenix = True


            # YARN Services

            if service == 'RESOURCEMANAGER':
                logging.debug("Found YARN Resource Manager on Host {0}...".format(self.name))
                self.resourcemanager = True

            if service == 'NODEMANAGER':
                logging.debug("Found YARN Node Manager on Host {0}...".format(self.name))
                self.nodemanager = True

            if service == 'APP_TIMELINE_SERVER':
                logging.debug("Found YARN App Timeline Server on Host {0}...".format(self.name))
                self.apptimeline = True


            # Other Hadoop services

            if service == 'ZOOKEEPER':
                logging.debug("Found ZooKeeper on Host {0}...".format(self.name))
                self.zookeeper = True

            if service == 'OOZIE_SERVER':
                logging.debug("Found Oozie on Host {0}...".format(self.name))
                self.oozie = True

            if service == 'SPARK_JOBHISTORYSERVER':
                logging.debug("Found Spark History on Host {0}...".format(self.name))
                self.sparkhistory = True

            if service == 'KAFKA':
                logging.debug("Found Kafka on Host {0}...".format(self.name))
                self.kafka = True


class HadoopHost(object):
    """
    Parent Class for various types of Hadoop Hosts
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
        self.START = {
            "RequestInfo": {
                "context": "Start {0} via REST".format(self.description)
            },
            "Body": {
                "HostRoles": {
                    "state": "STARTED"
                }
            }
        }
        self.STOP = {
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
        """
        logging.info("Stopping {0} on {1}...".format(self.description, self.fqdn))
        return self.ambari.execute(self.ambari_url, self.STOP)

    def start(self):
        """
        """
        logging.info("Starting {0} on {1}...".format(self.description, self.fqdn))
        return self.ambari.execute(self.ambari_url, self.START)

    def tcp_port_open(self):
        """
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((self.fqdn, self.port))

        if result == 0:
            logging.debug("TCP port {0} for {1} on {2} is open.".format(self.port, self.description, self.fqdn))
            return True
        else:
            logging.warn("TCP port on {0} for {1} on {2} is closed.".format(self.port, self.description, self.fqdn))
            return False

    def tcp_port_closed(self):
        """
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((self.fqdn, self.port))

        if result == 0:
            logging.debug("TCP port {0} for {1} on {2} is open.".format(self.port, self.description, self.fqdn))
            return False
        else:
            logging.debug("TCP port on {0} for {1} on {2} is closed.".format(self.port, self.description, self.fqdn))
            return True


    def is_healthy(self):
        """
        Public method to query the Health of the Service
        """
        return self.tcp_port_open()



class HadoopService(object):
    """
    Class to define service-wide properties and actions
    """
    def __init__(self, host, service):
        """
        """
        self.service = service
        self.host = host
        self.ambari = host.ambari

        request_uri = '/requests/'
        self.ambari_url = self.ambari.url + request_uri

        self.REFRESH_CONFIGS = {
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

    def refresh(self):
        """
        """
        logging.info("Refreshing client configs for {0} on {1}...".format(self.service, self.host.fqdn))
        return self.ambari.queue(self.ambari_url, self.REFRESH_CONFIGS)



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
        Connects to the NameNode to get JMX data in JSON format
        """
        try:
            return requests.get(url).content
        except:
            logging.warning('JMX Data not returned from {1}, Retrying...'.format(url))
            time.sleep(HTTP_RETRY_DELAY)
            return self.get_jmx(url)


    def get_beans(self, jmx_data):
        """
        """
        return json.loads(jmx_data.decode('utf8'))['beans']


    def get_jmx_dict(self, jmx_url):
        """
        """
        return self.get_beans(self.get_jmx(jmx_url))


class NameNode(JmxHadoopHost):
    """
    HDFS NameNode host
    """
    def __init__(self, host, port = 50070):
        """
        """
        self.description = "HDFS NameNode"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(NameNode, self).__init__(host, port)

        namenode_uri = '/hosts/' + self.fqdn + '/host_components/NAMENODE'
        self.ambari_url = self.ambari.url + namenode_uri

        self.state = self.__get_state(self.jmx_dict)
        self.safemode = self.__get_safemode(self.jmx_dict)
        self.__set_status(self.jmx_dict)

        logging.debug("Initialization of {0} complete.".format(self.description))


    def is_healthy(self):
        """
        Public method to query the Health of the NameNode
        """
        return self.get_safemode()


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
                        logging.debug("NameNode: state = {}".format(bean['State'].upper()))
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
        logging.debug("NameNode: livenodes = {0}".format(self.livenodes))
        return self.livenodes

    def __set_status(self, jmx_dict):
        """
        Private method to set variables based on JMX data
        """
        self.get_livenodes()
        for bean in jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=NameNode,name=NameNodeStatus':
                        if bean['SecurityEnabled'] == 'true':
                            self.security_enabled = True
                        else:
                            self.security_enabled = False
                        logging.debug("NameNode: security_enabled = {0}".format(self.security_enabled))

                    if bean[key] == 'Hadoop:service=NameNode,name=StartupProgress':
                        self.startup_percent_complete = float(bean['PercentComplete'])
                        logging.debug("NameNode: startup_percent_complete = {0}".format(self.startup_percent_complete))
                        self.loading_edits_percent_complete = float(bean['LoadingEditsPercentComplete'])
                        logging.debug("NameNode: loading_edits_percent_complete = {0}".format(self.loading_edits_percent_complete))

                    if bean[key] == 'Hadoop:service=NameNode,name=FSNamesystem':
                        self.missing_blocks = int(bean['MissingBlocks'])
                        logging.debug("NameNode: missing_blocks = {0}".format(self.missing_blocks))
                        self.missing_repl_one_blocks = int(bean['MissingReplOneBlocks'])
                        logging.debug("NameNode: missing_repl_one_blocks = {0}".format(self.missing_repl_one_blocks))
                        self.expired_heartbeats = int(bean['ExpiredHeartbeats'])
                        logging.debug("NameNode: expired_heartbeats = {0}".format(self.expired_heartbeats))
                        self.lock_queue_length = int(bean['LockQueueLength'])
                        logging.debug("NameNode: lock_queue_length = {0}".format(self.lock_queue_length))
                        self.num_active_clients = int(bean['NumActiveClients'])
                        logging.debug("NameNode: num_active_clients = {0}".format(self.num_active_clients))
                        self.pending_replication_blocks = int(bean['PendingReplicationBlocks'])
                        logging.debug("NameNode: pending_replication_blocks = {0}".format(self.pending_replication_blocks))
                        self.under_replicated_blocks = int(bean['UnderReplicatedBlocks'])
                        logging.debug("NameNode: under_replicated_blocks = {0}".format(self.under_replicated_blocks))
                        self.corrupt_blocks = int(bean['CorruptBlocks'])
                        logging.debug("NameNode: corrupt_blocks = {0}".format(self.corrupt_blocks))
                        self.scheduled_replication_blocks = int(bean['ScheduledReplicationBlocks'])
                        logging.debug("NameNode: scheduled_replication_blocks = {0}".format(self.scheduled_replication_blocks))
                        self.pending_deletion_blocks = int(bean['PendingDeletionBlocks'])
                        logging.debug("NameNode: pending_deletion_blocks = {0}".format(self.pending_deletion_blocks))
                        self.excess_blocks = int(bean['ExcessBlocks'])
                        logging.debug("NameNode: excess_blocks = {0}".format(self.excess_blocks))
                        self.postponed_misreplicated_blocks = int(bean['PostponedMisreplicatedBlocks'])
                        logging.debug("NameNode: postponed_misreplicated_blocks = {0}".format(self.postponed_misreplicated_blocks))
                        self.pending_datanode_msg_count = int(bean['PendingDataNodeMessageCount'])
                        logging.debug("NameNode: pending_datanode_msg_count = {0}".format(self.pending_datanode_msg_count))
                        self.stale_datanodes = int(bean['StaleDataNodes'])
                        logging.debug("NameNode: stale_datanodes = {0}".format(self.stale_datanodes))

                    if bean[key] == 'Hadoop:service=NameNode,name=NameNodeInfo':
                        self.threads = int(bean['Threads'])
                        if bean['UpgradeFinalized'] == 'true':
                            self.finalized = True
                        else:
                            self.finalized = False
                        logging.debug("NameNode: finalized = {0}".format(self.finalized))
                        self.total_blocks = int(bean['TotalBlocks'])
                        logging.debug("NameNode: total_blocks = {0}".format(self.total_blocks))
                        self.total_files = int(bean['TotalFiles'])
                        logging.debug("NameNode: total_files = {0}".format(self.total_files))



class ZkFailoverController(HadoopHost):
    """
    HDFS ZooKeeper Failover Controller hosts which coexist on
    NameNode servers that support HA
    """
    def __init__(self, host, port = 8019):
        """
        """
        self.description = "HDFS Failover Controller"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(ZkFailoverController, self).__init__(host, port)

        zkfc_uri = '/hosts/' + self.fqdn + '/host_components/ZKFC'
        self.ambari_url = self.ambari.url + zkfc_uri

        logging.debug("Initialization of {0} complete.".format(self.description))



class DataNode(JmxHadoopHost):
    """
    HDFS DataNode host
    """
    def __init__(self, host, port = 50075):
        """
        """
        self.description = "HDFS DataNode"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(DataNode, self).__init__(host, port)

        datanode_uri = '/hosts/' + self.fqdn + '/host_components/DATANODE'
        self.ambari_url = self.ambari.url + datanode_uri
        self.__set_status(self.jmx_dict)

        logging.debug("Initialization of {0} complete.".format(self.description))


    def is_healthy(self):
        """
        Public method to query the Health of the DataNode
        """
        self.get_status()
        return self.failed_volumes == 0 and len(self.namenodes) > 1


    def get_status(self, jmx_dict):
        """
        Public method to set variables based on JMX data
        """
        self.jmx_dict = self.get_jmx_dict(self.jmx_url)
        self.__set_status(self.jmx_dict)


    def __set_status(self, jmx_dict):
        """
        Private method to set variables based on JMX data
        """
        for bean in jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=DataNode,name=FSDatasetState':
                        self.failed_volumes = int(bean['NumFailedVolumes'])
                        logging.debug("DataNode: failed_volumes = {0}".format(self.failed_volumes))
                        self.capacity = int(bean['Capacity'])
                        logging.debug("DataNode: capacity = {0}".format(self.capacity))
                        self.used = int(bean['DfsUsed'])
                        logging.debug("DataNode: used = {0}".format(self.used))
                        self.remaining = int(bean['Remaining'])
                        logging.debug("DataNode: remaining = {0}".format(self.remaining))
                        self.lost_capacity = int(bean['EstimatedCapacityLostTotal'])
                        logging.debug("DataNode: lost_capacity = {0}".format(self.lost_capacity))

                    if bean[key] == 'Hadoop:service=DataNode,name=DataNodeInfo':
                        self.namenodes = []
                        for nn in json.loads(bean['NamenodeAddresses']).keys():
                            self.namenodes.append(nn.split('.')[0])
                        logging.debug("DataNode: namenodes = {0}".format(self.namenodes))

                    if bean[key] == 'Hadoop:service=DataNode,name=JvmMetrics':
                        self.blocked_threads = int(bean['ThreadsBlocked'])
                        logging.debug("DataNode: blocked_threads = {0}".format(self.blocked_threads))



class JournalNode(JmxHadoopHost):
    """
    HDFS Journal Node
    """
    def __init__(self, host, port = 8480):
        """
        """
        self.description = "HDFS Journal"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(JournalNode, self).__init__(host, port)

        journalnode_uri = '/hosts/' + self.fqdn + '/host_components/JOURNALNODE'
        self.ambari_url = self.ambari.url + journalnode_uri

        logging.debug("Initialization of {0} complete.".format(self.description))


class NfsGateway(JmxHadoopHost):
    """
    NFS Gateway for HDFS
    """
    def __init__(self, host, port = 50079):
        """
        """
        self.description = "NFS Gateway"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(NfsGateway, self).__init__(host, port)

        nfsgateway_uri = '/hosts/' + self.fqdn + '/host_components/NFS_GATEWAY'
        self.ambari_url = self.ambari.url + nfsgateway_uri

        logging.debug("Initialization of {0} complete.".format(self.description))


class ResourceManager(JmxHadoopHost):
    """
    YARN ResourceManager
    """
    def __init__(self, host, port = 8088):
        """
        """
        self.description = "YARN ResourceManager"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(ResourceManager, self).__init__(host, port)


        resourcemanager_uri = '/hosts/' + self.fqdn + '/host_components/RESOURCEMANAGER'
        self.ambari_url = self.ambari.url + resourcemanager_uri
        self.__set_status(self.jmx_dict)
        self.get_state()
        self.get_livenodes()

        logging.debug("Initialization of {0} complete.".format(self.description))

    def get_state(self):
        url = 'http://{0}:{1}/cluster'.format(self.fqdn, self.port)
        try:
            r = requests.get(url, allow_redirects=False)
            if r.status_code == 302 or r.status_code == 307:
                self.state = 'standby'
                return self.state
            elif r.status_code == 200:
                self.state = 'active'
                return self.state
            else:
                logging.error('HTTP GET to {0} returned {1} with contents: {2}'.format(url, r.status_code, r.content))
                time.sleep(HTTP_RETRY_DELAY)
                return self.get_state()
        except:
            logging.warning('HTML Data for {0} not returned from {1}, Retrying...'.format(self.fqdn, url))
            time.sleep(HTTP_RETRY_DELAY)
            return self.get_state()


    def get_livenodes(self):
        """
        """
        livenodes = []
        for bean in self.jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=ResourceManager,name=RMNMInfo':
                        for node in json.loads(bean['LiveNodeManagers']):
                            # list of dicts?
                            # For a dict, Key is HostName
                            name = node['HostName'].split('.')[0]
                            livenodes.append(name)
        self.livenodes = sorted(livenodes)
        logging.debug("ResourceManager: livenodes = {0}".format(self.livenodes))
        return self.livenodes



    def __set_status(self, jmx_dict):
        """
        Private method to set variables based on JMX data
        """
        for bean in jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=ResourceManager,name=MetricsSystem,sub=Stats':
                        self.num_active_sources = int(bean['NumActiveSources'])
                        logging.debug("ResourceManager: num_active_sources = {0}".format(self.num_active_sources))
                        self.num_all_sources = int(bean['NumAllSources'])
                        logging.debug("ResourceManager: num_all_sources = {0}".format(self.num_all_sources))
                        self.num_active_sinks = int(bean['NumActiveSinks'])
                        logging.debug("ResourceManager: num_active_sinks = {0}".format(self.num_active_sinks))

                    if bean[key] == 'Hadoop:service=ResourceManager,name=ClusterMetrics':
                        self.num_active_nodemanagers = int(bean['NumActiveNMs'])
                        logging.debug("ResourceManager: num_active_nodemanagers = {0}".format(self.num_active_nodemanagers))
                        self.num_decommissioned_nodemanagers = int(bean['NumDecommissionedNMs'])
                        logging.debug("ResourceManager: num_decommissioned_nodemanagers = {0}".format(self.num_decommissioned_nodemanagers))
                        self.num_lost_nodemanagers = int(bean['NumLostNMs'])
                        logging.debug("ResourceManager: num_lost_nodemanagers = {0}".format(self.num_lost_nodemanagers))
                        self.num_unhealthy_nodemanagers = int(bean['NumUnhealthyNMs'])
                        logging.debug("ResourceManager: num_unhealthy_nodemanagers = {0}".format(self.num_unhealthy_nodemanagers))
                        self.num_rebooted_nodemanagers = int(bean['NumRebootedNMs'])
                        logging.debug("ResourceManager: num_rebooted_nodemanagers = {0}".format(self.num_rebooted_nodemanagers))






class NodeManager(JmxHadoopHost):
    """
    YARN NodeManger
    """
    def __init__(self, host, port = 8042):
        """
        """
        self.description = "YARN NodeManger"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(NodeManager, self).__init__(host, port)

        nodemanager_uri = '/hosts/' + self.fqdn + '/host_components/NODEMANAGER'
        self.ambari_url = self.ambari.url + nodemanager_uri
        self.__set_status(self.jmx_dict)

        logging.debug("Initialization of {0} complete.".format(self.description))


    def __set_status(self, jmx_dict):
        """
        Private method to set variables based on JMX data
        """
        for bean in jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=NodeManager,name=NodeManagerMetrics':
                        self.containers_initing = int(bean['ContainersIniting'])
                        logging.debug("NodeManager: containers_initing = {0}".format(self.containers_initing))
                        self.containers_running = int(bean['ContainersRunning'])
                        logging.debug("NodeManager: containers_running = {0}".format(self.containers_running))
                        self.bad_local_dirs = int(bean['BadLocalDirs'])
                        logging.debug("NodeManager: bad_local_dirs = {0}".format(self.bad_local_dirs))
                        self.bad_log_dirs = int(bean['BadLogDirs'])
                        logging.debug("NodeManager: bad_log_dirs = {0}".format(self.bad_log_dirs))

                    if bean[key] == 'Hadoop:service=NodeManager,name=ShuffleMetrics':
                        self.shuffle_outputs_failed = int(bean['ShuffleOutputsFailed'])
                        logging.debug("NodeManager: shuffle_outputs_failed = {0}".format(self.shuffle_outputs_failed))
                        self.shuffle_connections = int(bean['ShuffleConnections'])
                        logging.debug("NodeManager: shuffle_connections = {0}".format(self.shuffle_connections))

                    if bean[key] == 'Hadoop:service=NodeManager,name=MetricsSystem,sub=Stats':
                        self.num_active_sources = int(bean['NumActiveSources'])
                        logging.debug("NodeManager: num_active_sources = {0}".format(self.num_active_sources))
                        self.num_all_sources = int(bean['NumAllSources'])
                        logging.debug("NodeManager: num_all_sources = {0}".format(self.num_all_sources))
                        self.num_active_sinks = int(bean['NumActiveSinks'])
                        logging.debug("NodeManager: num_active_sinks = {0}".format(self.num_active_sinks))
                        self.sink_timeline_dropped = int(bean['Sink_timelineDropped'])
                        logging.debug("NodeManager: sink_timeline_dropped = {0}".format(self.sink_timeline_dropped))
                        self.sink_timeline_q_size = int(bean['Sink_timelineQsize'])
                        logging.debug("NodeManager: sink_timeline_q_size = {0}".format(self.sink_timeline_q_size))




class AppTimeline(JmxHadoopHost):
    """
    App Timeline server for YARN
    """
    def __init__(self, host, port = 8188):
        """
        """
        self.description = "App Timeline Server"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(AppTimeline, self).__init__(host, port)

        apptimeline_uri = '/hosts/' + self.fqdn + '/host_components/APP_TIMELINE_SERVER'
        self.ambari_url = self.ambari.url + apptimeline_uri

        logging.debug("Initialization of {0} complete.".format(self.description))


class HbaseMaster(JmxHadoopHost):
    """
    HBase Master server
    """
    def __init__(self, host, port = 16010):
        """
        """
        self.description = "HBase Master"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(HbaseMaster, self).__init__(host, port)

        hbasemaster_uri = '/hosts/' + self.fqdn + '/host_components/HBASE_MASTER'
        self.ambari_url = self.ambari.url + hbasemaster_uri

        self.state = self.__get_state(self.jmx_dict)
        self.__set_status(self.jmx_dict)

        logging.debug("Initialization of {0} complete.".format(self.description))


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
                        logging.debug("HBaseMaster: state = {}".format(state.upper()))
                        return state


    def __set_status(self, jmx_dict):
        """
        Private method to set variables based on JMX data
        """
        for bean in jmx_dict:
            for key in bean.keys():
                if key == 'name':
                    if bean[key] == 'Hadoop:service=HBase,name=Master,sub=Balancer':
                        self.balancer_num_ops = int(bean['BalancerCluster_num_ops'])
                        logging.debug("HBaseMaster: balancer_num_ops = {0}".format(self.balancer_num_ops))
                        self.balancer_min = int(bean['BalancerCluster_min'])
                        logging.debug("HBaseMaster: balancer_min = {0}".format(self.balancer_min))
                        self.balancer_max = int(bean['BalancerCluster_max'])
                        logging.debug("HBaseMaster: balancer_max = {0}".format(self.balancer_max))
                        self.balancer_mean = float(bean['BalancerCluster_mean'])
                        logging.debug("HBaseMaster: balancer_mean = {0}".format(self.balancer_mean))

                    if bean[key] == 'Hadoop:service=HBase,name=Master,sub=AssignmentManger':
                        self.rit_oldest_age = int(bean['ritOldestAge'])
                        logging.debug("HBaseMaster: rit_oldest_age = {0}".format(self.rit_oldest_age))
                        self.rit_count_over_threshold = int(bean['ritCountOverThreshold'])
                        logging.debug("HBaseMaster: rit_count_over_threshold = {0}".format(self.rit_count_over_threshold))
                        self.rit_count = int(bean['ritCount'])
                        logging.debug("HBaseMaster: rit_count = {0}".format(self.rit_count))
                        self.assign_num_ops = int(bean['Assign_num_ops'])
                        logging.debug("HBaseMaster: assign_num_ops = {0}".format(self.assign_num_ops))
                        self.assign_min = int(bean['Assign_min'])
                        logging.debug("HBaseMaster: assign_min = {0}".format(self.assign_min))
                        self.assign_max = int(bean['Assign_max'])
                        logging.debug("HBaseMaster: assign_max = {0}".format(self.assign_max))
                        self.assign_mean = float(bean['Assign_mean'])
                        logging.debug("HBaseMaster: assign_mean = {0}".format(self.assign_mean))

                    if bean[key] == 'Hadoop:service=HBase,name=Master,sub=Server':
                        if bean['tag.isActiveMaster'] == "true":
                            self.is_active_master = True
                        else:
                            self.is_active_master = False
                        logging.debug("HBaseMaster: is_active_master = {0}".format(self.is_active_master))
                        self.average_load = float(bean['averageLoad'])
                        logging.debug("HBaseMaster: average_load = {0}".format(self.average_load))
                        self.num_regionservers = int(bean['numRegionServers'])
                        logging.debug("HBaseMaster: num_regionservers = {0}".format(self.num_regionservers))
                        self.num_dead_regionservers = int(bean['numDeadRegionServers'])
                        logging.debug("HBaseMaster: num_dead_regionservers = {0}".format(self.num_dead_regionservers))



class RegionServer(JmxHadoopHost):
    """
    HBase Region server
    """
    def __init__(self, host, port = 16030):
        """
        """
        self.description = "HBase Region Server"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(RegionServer, self).__init__(host, port)

        regionserver_uri = '/hosts/' + self.fqdn + '/host_components/HBASE_REGIONSERVER'
        self.ambari_url = self.ambari.url + regionserver_uri

        logging.debug("Initialization of {0} complete.".format(self.description))


class PhoenixQueryServer(HadoopHost):
    """
    Phoenix Query server for HBase
    """
    def __init__(self, host, port = 8765):
        """
        """
        self.description = "Phoenix Query Server"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(PhoenixQueryServer, self).__init__(host, port)

        phoenixqueryserver_uri = '/hosts/' + self.fqdn + '/host_components/PHOENIX_QUERY_SERVER'
        self.ambari_url = self.ambari.url + phoenixqueryserver_uri

        logging.debug("Initialization of {0} complete.".format(self.description))



class JobHistory(JmxHadoopHost):
    """
    """
    def __init__(self, host, port = 19888):
        """
        """
        self.description = "MapReduce2 Job History"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(Oozie, self).__init__(host, port)

        jobhistory_uri = '/hosts/' + self.fqdn + '/host_components/HISTORYSERVER'
        self.ambari_url = self.ambari.url + jobhistory_uri

        logging.debug("Initialization of {0} complete.".format(self.description))



class Oozie(HadoopHost):
    """
    """
    def __init__(self, host, port = 11000):
        """
        """
        self.description = "Oozie Server"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(Oozie, self).__init__(host, port)

        oozie_uri = '/hosts/' + self.fqdn + '/host_components/OOZIE_SERVER'
        self.ambari_url = self.ambari.url + oozie_uri

        logging.debug("Initialization of {0} complete.".format(self.description))


class ZooKeeper(HadoopHost):
    """
    """
    def __init__(self, host, port = 2181):
        """
        """
        self.description = "ZooKeeper"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(Oozie, self).__init__(host, port)

        zookeeper_uri = '/hosts/' + self.fqdn + '/host_components/ZOOKEEPER_SERVER'
        self.ambari_url = self.ambari.url + zookeeper_uri

        logging.debug("Initialization of {0} complete.".format(self.description))



class SparkHistory(HadoopHost):
    """
    """
    def __init__(self, host, port = 18080):
        """
        """
        self.description = "Spark History"
        logging.debug("Initializing {0}: {1}...".format(self.description, host.name))
        super(SparkHistory, self).__init__(host, port)

        sparkhistory_uri = '/hosts/' + self.fqdn + '/host_components/SPARK_JOBHISTORYSERVER'
        self.ambari_url = self.ambari.url + sparkhistory_uri

        logging.debug("Initialization of {0} complete.".format(self.description))




@click.command()
@click.option('--hostname', prompt='Enter Ambari Hostname', help='The hostname of the Ambari server.')
@click.option('--username', prompt='Enter Admin Username', help='A user account that has admin privileges.')
@click.option('--password', prompt='Enter Admin Password', help='The password for the admin user.')
@click.option('--cluster', default='pdxlab', help='')
@click.option('--domain', default='pdx.lab.org', help='')
@click.option('--port', default=8080, help='')
@click.argument('service')
def init_script(hostname, username, password, cluster, domain, port, service):
    """
    """
    name = hostname.split('.')[0]

    logging.debug("===> Step 1. Connect to Ambari server and Discover Ambari-managed Hosts.")
    ambari = Ambari(name, username, password, cluster, domain, port)

    logging.debug("===> Step 2. Select the proper subroutine(s) to execute.")

    if service.upper() == 'HDFS' or service.upper() == 'ALL':
        restart_hdfs(ambari)

    if service.upper() == 'YARN' or service.upper() == 'ALL':
        restart_yarn(ambari)

    if service.upper() == 'HBASE' or service.upper() == 'ALL':
        restart_hbase(ambari)

    if service.upper() == 'MR2' or service.upper() == 'MAPREDUCE2' or service.upper() == 'ALL':
        restart_mr2(ambari)

    if service.upper() == 'TEZ' or service.upper() == 'ALL':
        restart_tez(ambari)

    if service.upper() == 'HIVE' or service.upper() == 'ALL':
        restart_hive(ambari)

    if service.upper() == 'PIG' or service.upper() == 'ALL':
        restart_pig(ambari)

    if service.upper() == 'OOZIE' or service.upper() == 'ALL':
        restart_oozie(ambari)

    if service.upper() == 'ZOOKEEPER' or service.upper() == 'ALL':
        restart_zookeeper(ambari)

    if service.upper() == 'SPARK' or service.upper() == 'ALL':
        restart_spark(ambari)

    if service.upper() == 'KAFKA' or service == 'ALL':
        restart_kafka(ambari)


def restart_hdfs(ambari):
    """
    Function which acts as a manifest to restart HDFS services
    """
    logging.debug("==> Beginning HDFS restart...")

    namenodes = []
    zkfcs = []
    journalnodes = []
    datanodes = []
    nfs_gateways = []

    for host in ambari.hosts:
        if host.namenode:
            if host.zkfc:
                namenodes.append(NameNode(host))
                zkfcs.append(ZkFailoverController(host))
            else:
                logging.error("NameNode on {0} does not have a Failover Controller!".format(host.name))
                raise Exception('Exiting...')

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
                logging.warn("{0} on {1} is in SafeMode. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)

            logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
            node.stop()
            time.sleep(NAMENODE_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(NAMENODE_RESTART_DELAY)

            # Do not proceed until the NameNode exits SafeMode
            while node.get_safemode():
                logging.warn("{0} on {1} is in SafeMode. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)

            # Do not proceed unless this NameNode is a Standby
            while node.get_state() != 'standby':
                logging.warn("{0} on {1} is not a Standby. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)

            for zkfc in zkfcs:
                if zkfc.fqdn == node.fqdn:
                    logging.info("Restarting {0} on {1}...".format(zkfc.description, zkfc.fqdn))
                    zkfc.stop()
                    time.sleep(NAMENODE_RESTART_DELAY)
                    while zkfc.tcp_port_closed():
                        zkfc.start()
                        time.sleep(NAMENODE_RESTART_DELAY)

            # Do not proceed until the NameNode exits SafeMode
            while node.get_safemode():
                logging.warn("{0} on {1} is in SafeMode. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)

            # Do not proceed unless this NameNode is a Standby
            while node.get_state() != 'standby':
                logging.warn("{0} on {1} is not a Standby. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)


    # Restart ONLY the Active NameNode
    for node in namenodes:
        if node.state == 'active':

            # Do not proceed until the NameNode exits SafeMode
            while node.get_safemode():
                logging.warn("{0} on {1} is in SafeMode. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)

            logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
            node.stop()
            time.sleep(NAMENODE_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(NAMENODE_RESTART_DELAY)

            # Do not proceed until the NameNode exits SafeMode
            while node.get_safemode():
                logging.warn("{0} on {1} is in SafeMode. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)

            # Do not proceed unless this NameNode is a Standby
            while node.get_state() != 'standby':
                logging.warn("{0} on {1} is not a Standby. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)

            for zkfc in zkfcs:
                if zkfc.fqdn == node.fqdn:
                    logging.info("Restarting {0} on {1}...".format(zkfc.description, zkfc.fqdn))
                    zkfc.stop()
                    time.sleep(NAMENODE_RESTART_DELAY)
                    while zkfc.tcp_port_closed():
                        zkfc.start()
                        time.sleep(NAMENODE_RESTART_DELAY)

            # Do not proceed until the NameNode exits SafeMode
            while node.get_safemode():
                logging.warn("{0} on {1} is in SafeMode. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)

            # Do not proceed unless this NameNode is a Standby
            while node.get_state() != 'standby':
                logging.warn("{0} on {1} is not a Standby. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)



    # Find the new Active NameNode
    for node in namenodes:
        if node.get_state() == 'active':
            active_namenode = node
    try:
        livenodes = active_namenode.get_livenodes()
    except:
        logging.error("There are no active NameNodes!")
        raise Exception('Exiting...')


    # Restart the HDFS JournalNodes
    for node in journalnodes:
        logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
        node.stop()
        time.sleep(JOURNALNODE_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(JOURNALNODE_RESTART_DELAY)


    # Restart the HDFS Datanodes
    for node in datanodes:
        if node.name in livenodes:
            logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
            node.stop()
            time.sleep(DATANODE_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(DATANODE_RESTART_DELAY)
        else:
            logging.error("DataNode on {0} is not in the LiveNodes list on the active NameNode".format(node.fqdn))
            raise Exception('Exiting...')

    for node in nfs_gateways:
        logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
        node.stop()
        time.sleep(DATANODE_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(DATANODE_RESTART_DELAY)

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info("Refreshing HDFS client configs on {0}...".format(host.fqdn))
        HadoopService(host, 'HDFS').refresh()



def restart_yarn(ambari):
    """
    Function which acts as a manifest to restart YARN services
    """
    logging.debug("==> Beginning YARN restart...")

    resourcemanagers = []
    nodemanagers = []
    apptimelines = []

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

            logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
            node.stop()
            time.sleep(RESOURCEMANAGER_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(RESOURCEMANAGER_RESTART_DELAY)

            # Do not proceed unless this ResourceManager is a Standby
            while node.get_state() != 'standby':
                logging.warn("{0} on {1} is not a Standby. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)


    # Now restart the Active Resource Manager
    for node in resourcemanagers:

        if node.state == 'active':

            logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
            node.stop()
            time.sleep(RESOURCEMANAGER_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(RESOURCEMANAGER_RESTART_DELAY)

            # Do not proceed unless this ResourceManager is a Standby
            while node.get_state() != 'standby':
                logging.warn("{0} on {1} is not a Standby. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)

    for node in nodemanagers:
        logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
        node.stop()
        time.sleep(NODEMANAGER_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(NODEMANAGER_RESTART_DELAY)

    for node in apptimelines:
        logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
        node.stop()
        time.sleep(NODEMANAGER_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(NODEMANAGER_RESTART_DELAY)

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info("Refreshing YARN client configs on {0}...".format(host.fqdn))
        HadoopService(host, 'YARN').refresh()


def restart_hbase(ambari):
    """
    Function which acts as a manifest to restart HBase services
    """
    logging.debug("==> Beginning HBase restart...")

    hbasemasters = []
    regionservers = []
    queryservers = []

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

            logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
            node.stop()
            time.sleep(HBASEMASTER_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(HBASEMASTER_RESTART_DELAY)

            # Do not proceed unless this HBase Master is a Standby
            while node.get_state() != 'standby':
                logging.warn("{0} on {1} is not a Standby. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)



    # Now restart the Active HBase Master
    for node in hbasemasters:

        if node.state == 'active':
    
            logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
            node.stop()
            time.sleep(HBASEMASTER_RESTART_DELAY)
            while node.tcp_port_closed():
                node.start()
                time.sleep(HBASEMASTER_RESTART_DELAY)

            # Do not proceed unless this HBase Master is a Standby
            while node.get_state() != 'standby':
                logging.warn("{0} on {1} is not a Standby. Retrying...".format(node.description, node.fqdn))
                time.sleep(SAFEMODE_RETRY_DELAY)


    for node in regionservers:
        logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
        node.stop()
        time.sleep(REGIONSERVER_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(REGIONSERVER_RESTART_DELAY)

    for node in queryservers:
        logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
        node.stop()
        time.sleep(REGIONSERVER_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(REGIONSERVER_RESTART_DELAY)

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info("Refreshing HBase client configs on {0}...".format(host.fqdn))
        HadoopService(host, 'HBASE').refresh()


def restart_mr2(ambari):
    pass


def restart_tez(ambari):

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info("Refreshing Tez client configs on {0}...".format(host.fqdn))
        HadoopService(host, 'TEZ').refresh()


def restart_hive(ambari):
    pass


def restart_pig(ambari):
    pass


def restart_oozie(ambari):

    oozies = []

    for host in ambari.hosts:

        if host.oozie:
            oozies.append(Oozie(host))

    for node in oozies:
        logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
        node.stop()
        time.sleep(OOZIE_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(OOZIE_RESTART_DELAY)

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info("Refreshing Oozie client configs on {0}...".format(host.fqdn))
        HadoopService(host, 'OOZIE').refresh()


def restart_zookeeper(ambari):

    zookeepers = []

    for host in ambari.hosts:

        if host.zookeeper:
            zookeepers.append(ZooKeeper(host))

    for node in zookeepers:
        logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
        node.stop()
        time.sleep(ZOOKEEPER_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(ZOOKEEPER_RESTART_DELAY)

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info("Refreshing ZooKeeper client configs on {0}...".format(host.fqdn))
        HadoopService(host, 'ZOOKEEPER').refresh()


def restart_spark(ambari):

    sparkhistories = []

    for host in ambari.hosts:

        if host.sparkhistory:
            sparkhistories.append(SparkHistory(host))

    for node in sparkhistories:
        logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
        node.stop()
        time.sleep(SPARKHISTORY_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(SPARKHISTORY_RESTART_DELAY)

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info("Refreshing Spark client configs on {0}...".format(host.fqdn))
        HadoopService(host, 'SPARK').refresh()


def restart_kafka(ambari):

    kafkas = []

    for host in ambari.hosts:

        if host.kafka:
            kafkas.append(Kafka(host))

    for node in kafkas:
        logging.info("Restarting {0} on {1}...".format(node.description, node.fqdn))
        node.stop()
        time.sleep(KAFKA_RESTART_DELAY)
        while node.tcp_port_closed():
            node.start()
            time.sleep(KAFKA_RESTART_DELAY)

    # Refresh the client configs
    for host in ambari.hosts:
        logging.info("Refreshing Kafka client configs on {0}...".format(host.fqdn))
        HadoopService(host, 'KAFKA').refresh()




if __name__ == '__main__':
    """
    Initializes the script if it is being called directly
    """
    init_script()


