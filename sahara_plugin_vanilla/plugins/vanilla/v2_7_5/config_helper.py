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

import copy

from oslo_config import cfg
import six

from sahara.plugins import provisioning as p
from sahara.plugins import utils
from sahara_plugin_vanilla.plugins.vanilla.hadoop2 import config_helper

CONF = cfg.CONF
CONF.import_opt("enable_data_locality", "sahara.topology.topology_helper")

CORE_DEFAULT = utils.load_hadoop_xml_defaults(
    'plugins/vanilla/v2_7_5/resources/core-default.xml',
    'sahara_plugin_vanilla')

HDFS_DEFAULT = utils.load_hadoop_xml_defaults(
    'plugins/vanilla/v2_7_5/resources/hdfs-default.xml',
    'sahara_plugin_vanilla')

MAPRED_DEFAULT = utils.load_hadoop_xml_defaults(
    'plugins/vanilla/v2_7_5/resources/mapred-default.xml',
    'sahara_plugin_vanilla')

YARN_DEFAULT = utils.load_hadoop_xml_defaults(
    'plugins/vanilla/v2_7_5/resources/yarn-default.xml',
    'sahara_plugin_vanilla')

OOZIE_DEFAULT = utils.load_hadoop_xml_defaults(
    'plugins/vanilla/v2_7_5/resources/oozie-default.xml',
    'sahara_plugin_vanilla')

HIVE_DEFAULT = utils.load_hadoop_xml_defaults(
    'plugins/vanilla/v2_7_5/resources/hive-default.xml',
    'sahara_plugin_vanilla')

_default_executor_classpath = ":".join(
    ['/opt/hadoop/share/hadoop/tools/lib/hadoop-openstack-2.7.5.jar'])

SPARK_CONFS = copy.deepcopy(config_helper.SPARK_CONFS)

SPARK_CONFS['Spark']['OPTIONS'].append(
    {
        'name': 'Executor extra classpath',
        'description': 'Value for spark.executor.extraClassPath'
                       ' in spark-defaults.conf'
                       ' (default: %s)' % _default_executor_classpath,
        'default': '%s' % _default_executor_classpath,
        'priority': 2,
    }
)

XML_CONFS = {
    "Hadoop": [CORE_DEFAULT],
    "HDFS": [HDFS_DEFAULT],
    "YARN": [YARN_DEFAULT],
    "MapReduce": [MAPRED_DEFAULT],
    "JobFlow": [OOZIE_DEFAULT],
    "Hive": [HIVE_DEFAULT]
}

ENV_CONFS = {
    "YARN": {
        'ResourceManager Heap Size': 1024,
        'NodeManager Heap Size': 1024
    },
    "HDFS": {
        'NameNode Heap Size': 1024,
        'SecondaryNameNode Heap Size': 1024,
        'DataNode Heap Size': 1024
    },
    "MapReduce": {
        'JobHistoryServer Heap Size': 1024
    },
    "JobFlow": {
        'Oozie Heap Size': 1024
    }
}

# Initialise plugin Hadoop configurations
PLUGIN_XML_CONFIGS = config_helper.init_xml_configs(XML_CONFS)
PLUGIN_ENV_CONFIGS = config_helper.init_env_configs(ENV_CONFS)


def _init_all_configs():
    configs = []
    configs.extend(PLUGIN_XML_CONFIGS)
    configs.extend(PLUGIN_ENV_CONFIGS)
    configs.extend(config_helper.PLUGIN_GENERAL_CONFIGS)
    configs.extend(_get_spark_configs())
    configs.extend(_get_zookeeper_configs())
    return configs


def _get_spark_opt_default(opt_name):
    for opt in SPARK_CONFS["Spark"]["OPTIONS"]:
        if opt_name == opt["name"]:
            return opt["default"]
    return None


def _get_spark_configs():
    spark_configs = []
    for service, config_items in six.iteritems(SPARK_CONFS):
        for item in config_items['OPTIONS']:
            cfg = p.Config(name=item["name"],
                           description=item["description"],
                           default_value=item["default"],
                           applicable_target=service,
                           scope="cluster", is_optional=True,
                           priority=item["priority"])
            spark_configs.append(cfg)
    return spark_configs


def _get_zookeeper_configs():
    zk_configs = []
    for service, config_items in six.iteritems(config_helper.ZOOKEEPER_CONFS):
        for item in config_items['OPTIONS']:
            cfg = p.Config(name=item["name"],
                           description=item["description"],
                           default_value=item["default"],
                           applicable_target=service,
                           scope="cluster", is_optional=True,
                           priority=item["priority"])
            zk_configs.append(cfg)
    return zk_configs


PLUGIN_CONFIGS = _init_all_configs()


def get_plugin_configs():
    return PLUGIN_CONFIGS


def get_xml_configs():
    return PLUGIN_XML_CONFIGS


def get_env_configs():
    return ENV_CONFS
