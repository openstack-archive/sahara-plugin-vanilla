# Copyright (c) 2015 Mirantis Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from oslo_config import cfg

from sahara.plugins import conductor
from sahara.plugins import context
from sahara.plugins import swift_helper
from sahara.plugins import utils
from sahara_plugin_vanilla.plugins.vanilla import abstractversionhandler as avm
from sahara_plugin_vanilla.plugins.vanilla.hadoop2 import config as c
from sahara_plugin_vanilla.plugins.vanilla.hadoop2 import keypairs
from sahara_plugin_vanilla.plugins.vanilla.hadoop2 import recommendations_utils
from sahara_plugin_vanilla.plugins.vanilla.hadoop2 import run_scripts as run
from sahara_plugin_vanilla.plugins.vanilla.hadoop2 import scaling as sc
from sahara_plugin_vanilla.plugins.vanilla.hadoop2 import starting_scripts
from sahara_plugin_vanilla.plugins.vanilla.hadoop2 import utils as u
from sahara_plugin_vanilla.plugins.vanilla.hadoop2 import validation as vl
from sahara_plugin_vanilla.plugins.vanilla import utils as vu
from sahara_plugin_vanilla.plugins.vanilla.v2_7_1 import config_helper
from sahara_plugin_vanilla.plugins.vanilla.v2_7_1 import edp_engine


CONF = cfg.CONF


class VersionHandler(avm.AbstractVersionHandler):
    def __init__(self):
        self.pctx = {
            'env_confs': config_helper.get_env_configs(),
            'all_confs': config_helper.get_plugin_configs()
        }

    def get_plugin_configs(self):
        return self.pctx['all_confs']

    def get_node_processes(self):
        return {
            "Hadoop": [],
            "MapReduce": ["historyserver"],
            "HDFS": ["namenode", "datanode", "secondarynamenode"],
            "YARN": ["resourcemanager", "nodemanager"],
            "JobFlow": ["oozie"],
            "Hive": ["hiveserver"],
            "Spark": ["spark history server"],
            "ZooKeeper": ["zookeeper"]
        }

    def validate(self, cluster):
        vl.validate_cluster_creating(self.pctx, cluster)

    def update_infra(self, cluster):
        pass

    def configure_cluster(self, cluster):
        c.configure_cluster(self.pctx, cluster)

    def start_cluster(self, cluster):
        keypairs.provision_keypairs(cluster)

        starting_scripts.start_namenode(cluster)
        starting_scripts.start_secondarynamenode(cluster)
        starting_scripts.start_resourcemanager(cluster)

        run.start_dn_nm_processes(utils.get_instances(cluster))
        run.await_datanodes(cluster)

        starting_scripts.start_historyserver(cluster)
        starting_scripts.start_oozie(self.pctx, cluster)
        starting_scripts.start_hiveserver(self.pctx, cluster)
        starting_scripts.start_zookeeper(cluster)

        swift_helper.install_ssl_certs(utils.get_instances(cluster))

        self._set_cluster_info(cluster)
        starting_scripts.start_spark(cluster)

    def decommission_nodes(self, cluster, instances):
        sc.decommission_nodes(self.pctx, cluster, instances)

    def validate_scaling(self, cluster, existing, additional):
        vl.validate_additional_ng_scaling(cluster, additional)
        vl.validate_existing_ng_scaling(self.pctx, cluster, existing)
        zk_ng = utils.get_node_groups(cluster, "zookeeper")
        if zk_ng:
            vl.validate_zookeeper_node_count(zk_ng, existing, additional)

    def scale_cluster(self, cluster, instances):
        keypairs.provision_keypairs(cluster, instances)
        sc.scale_cluster(self.pctx, cluster, instances)

    def _set_cluster_info(self, cluster):
        nn = vu.get_namenode(cluster)
        rm = vu.get_resourcemanager(cluster)
        hs = vu.get_historyserver(cluster)
        oo = vu.get_oozie(cluster)
        sp = vu.get_spark_history_server(cluster)
        info = {}

        if rm:
            info['YARN'] = {
                'Web UI': 'http://%s:%s' % (rm.get_ip_or_dns_name(), '8088'),
                'ResourceManager': 'http://%s:%s' % (
                    rm.get_ip_or_dns_name(), '8032')
            }

        if nn:
            info['HDFS'] = {
                'Web UI': 'http://%s:%s' % (nn.get_ip_or_dns_name(), '50070'),
                'NameNode': 'hdfs://%s:%s' % (nn.hostname(), '9000')
            }

        if oo:
            info['JobFlow'] = {
                'Oozie': 'http://%s:%s' % (oo.get_ip_or_dns_name(), '11000')
            }

        if hs:
            info['MapReduce JobHistory Server'] = {
                'Web UI': 'http://%s:%s' % (hs.get_ip_or_dns_name(), '19888')
            }

        if sp:
            info['Apache Spark'] = {
                'Spark UI': 'http://%s:%s' % (sp.management_ip, '4040'),
                'Spark History Server UI':
                    'http://%s:%s' % (sp.management_ip, '18080')
            }

        ctx = context.ctx()
        conductor.cluster_update(ctx, cluster, {'info': info})

    def get_edp_engine(self, cluster, job_type):
        if job_type in edp_engine.EdpOozieEngine.get_supported_job_types():
            return edp_engine.EdpOozieEngine(cluster)
        if job_type in edp_engine.EdpSparkEngine.get_supported_job_types():
            return edp_engine.EdpSparkEngine(cluster)

        return None

    def get_edp_job_types(self):
        return (edp_engine.EdpOozieEngine.get_supported_job_types() +
                edp_engine.EdpSparkEngine.get_supported_job_types())

    def get_edp_config_hints(self, job_type):
        return edp_engine.EdpOozieEngine.get_possible_job_config(job_type)

    def on_terminate_cluster(self, cluster):
        u.delete_oozie_password(cluster)
        keypairs.drop_key(cluster)

    def get_open_ports(self, node_group):
        return c.get_open_ports(node_group)

    def recommend_configs(self, cluster, scaling):
        recommendations_utils.recommend_configs(cluster,
                                                self.get_plugin_configs(),
                                                scaling)
