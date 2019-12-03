from datetime import datetime, timedelta
import os
import re

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.sensors import TimeDeltaSensor
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory


PIPELINE = "pipe_naf_reader"


class NAFReaderDagFactory(DagFactory):

    def __init__(self, country, pipeline=PIPELINE, **kwargs):
        super(NAFReaderDagFactory, self).__init__(pipeline=pipeline, **kwargs)
        self.country = country

    def get_dag_id_by_country(self, prefix, country_name):
        return '{}.{}'.format(prefix, country_name)

    def build(self, dag_id):
        if self.schedule_interval != '@daily':
            raise ValueError('Unsupported schedule interval {}'.format(self.schedule_interval))

        self.config['source_path'] = self.country['gcs_source']
        config = self.config
        name = self.country['name']
        dag_id=self.get_dag_id_by_country(dag_id, name)

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:

            wait_a_day = TimeDeltaSensor(
                task_id="wait_a_day",
                poke_interval=43200, #try every 12 hours
                delta=timedelta(days=1) #we want to have a delay of 1 so we have all messages
            )

            naf_reader = KubernetesPodOperator(
                namespace = os.getenv('K8_NAMESPACE'),
                image = '{docker_image}'.format(**config),
                cmds = ['./scripts/run.sh',
                    'naf_reader_daily',
                    '{name}'.format(**self.country),
                    '{gcs_source}'.format(**self.country),
                    '{gcs_csv_output}'.format(**self.country),
                    '{bq_output}'.format(**self.country),
                    '{ds}'.format(**config),
                    '{schema_file_name}'.format(**self.country)
                ],
                name = 'naf-reader-{}'.format(name),
                task_id = "naf-reader-task",
                get_logs = True,
                in_cluster = True if os.getenv('KUBERNETES_SERVICE_HOST') else False,
                dag = dag
            )

            generate_partitioned_table = KubernetesPodOperator(
                namespace = os.getenv('K8_NAMESPACE'),
                image = '{docker_image}'.format(**config),
                cmds = ['./scripts/run.sh',
                    'generate_partitioned_table_daily',
                    '{name}'.format(**self.country),
                    '{bq_output}'.format(**self.country),
                    '{bq_partitioned_output}'.format(**self.country),
                    '{ds}'.format(**config)
                ],
                name = 'naf-reader-generate-partitioned-table-{}'.format(name),
                task_id = "generate-partitioned-table-task",
                get_logs = True,
                in_cluster = True if os.getenv('KUBERNETES_SERVICE_HOST') else False,
                dag = dag,
                pool='k8operators_limit'
            )

            for sensor in self.source_sensors(dag):
                dag >> wait_a_day >> sensor >> naf_reader >> generate_partitioned_table

            return dag

country_configurations = config_tools.load_config(PIPELINE)['configurations']
for country_config in country_configurations:
    reader = NAFReaderDagFactory(country_config)
    dag_id = '{}_daily'.format(PIPELINE)
    globals()[reader.get_dag_id_by_country(dag_id, country_config['name'])]= reader.build(dag_id=dag_id)
