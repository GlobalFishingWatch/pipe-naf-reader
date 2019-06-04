from datetime import datetime, timedelta
import os
import json

from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory


PIPELINE = "pipe_naf_reader"

class GoogleCloudStoragePrefixSensor(BaseSensorOperator):
    """
    Checks for the existence of a file in Google Cloud Storage.
    """
    template_fields = ('bucket', 'prefix')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
            self,
            bucket,
            prefix,
            google_cloud_conn_id='google_cloud_storage_default',
            delegate_to=None,
            *args,
            **kwargs):

        super(GoogleCloudStoragePrefixSensor, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to

    def poke(self, context):
        self.log.info('Sensor checks existence of : %s, %s', self.bucket, self.prefix)
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to)
        return len(hook.list(self.bucket, prefix=self.prefix)) > 0


def source_exists_sensor(bucket, prefix, dag):
    return GoogleCloudStoragePrefixSensor(
        dag=dag,
        task_id='source_exists',
        bucket=bucket,
        prefix=prefix,
        poke_interval=10,   # check every 10 seconds for a minute
        timeout=60,
        retries=24*7,       # retry once per hour for a week
        retry_delay=timedelta(minutes=60),
        retry_exponential_backoff=False
    )


class NAFReaderDagFactory(DagFactory):

    def __init__(self, country, pipeline=PIPELINE, **kwargs):
        super(NAFReaderDagFactory, self).__init__(pipeline=pipeline, **kwargs)
        self.country = country

    def build(self, dag_id):
        if self.schedule_interval != '@daily':
            raise ValueError('Unsupported schedule interval {}'.format(self.schedule_interval))

        config = self.config
        name = self.config['name']
        dag_id='{}.{}'.format(dag_id, name)

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:

            # Checks that the gcs folder exists and contains file for the current {ds}
            # Question? must be inside the k8s op
            source_bucket = re.search('(?<=gs://)[^/]*', self.country['gcs_source']).group(0)
            naf_gcs_path = re.search('(?<=gs://)[^/]*/(.*)', self.country['gcs_source']).group(1)
            naf_gcs_path = '{}/{ds}'.format(naf_gcs_path, **config)
            source_exists = source_exists_sensor(
                bucket=source_bucket,
                prefix=naf_gcs_path,
                dag=dag
            )

            naf_reader = KubernetesPodOperator(
                namespace = os.getenv('K8_NAMESPACE'),
                image = '{docker_image}'.format(**config),
                cmds = './scripts/run.sh '
                    'naf_reader_daily '
                    '{name} '
                    '{gcs_source} '
                    '{gcs_csv_output} '
                    '{bq_output} '
                    '{ds}'.format(**config),
                name = 'naf_reader.{}'.format(name),
                task_id = "naf-reader-task",
                get_logs = True,
                in_cluster = True if os.getenv('KUBERNETES_SERVICE_HOST') else False,
                dag = dag
            )

            dag >> naf_reader >> source_exists

            return dag

country_configurations = json.loads(config_tools.load_config(pipeline)['configurations'])
for country_config in country_configurations:
    naf_reader_daily_dag = NAFReaderDagFactory(country_config).build(dag_id='{}_daily'.format(PIPELINE))
