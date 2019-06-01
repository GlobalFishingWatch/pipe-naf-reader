from datetime import datetime, timedelta
import os
import json

from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator

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

    def __init__(self, pipeline=PIPELINE, **kwargs):
        super(NAFReaderDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def source_date(self):
        if self.schedule_interval == '@daily':
            return '{{ ds }}','{{ ds_nodash }}'
        else:
            raise ValueError('Unsupported schedule interval {}'.format(self.schedule_interval))

    def build(self, dag_id):
        config = self.config
        source_date = self.source_date()
        config['date'] = source_date[0]
        config['date_nodash'] = source_date[1]
        print('config = %s' % (config))
        country_configurations = json.loads(config['configurations'])


        naf_reader_image = "{docker_image}".format(**config)
        print('naf_reader_image = %s' % naf_reader_image)

        dags=[]
        for country_config in country_configurations:
            dag_id='{}.{}'.format(dag_id)
            naf_reader_args = ['{name}',
                               '{gcs_source}',
                               '{gcs_csv_output}',
                               '{bq_output}',
                               '{ds}']
            for arg in range(len(naf_reader_args)):
                naf_reader_args[arg]=naf_reader_args[arg].format(**config)
            naf_reader_args.insert(0, 'naf_reader_daily')
            print('naf_reader_args = %s' % naf_reader_args)
            naf_reader_args.insert(0, "./scripts/run.sh")

            with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:

                # Checks that the gcs folder exists and contains file for the current {ds}
                # Question? must be inside the k8s op
                source_bucket = re.search('(?<=gs://)[^/]*', country_config['gcs_source']).group(0)
                naf_gcs_path = re.search('(?<=gs://)[^/]*/(.*)', country_config['gcs_source']).group(1)
                naf_gcs_path = '{}/{}'.format(naf_gcs_path, config['ds'])
                source_exists = source_exists_sensor(
                    bucket=source_bucket,
                    prefix=naf_gcs_path,
                    dag=dag
                )

                naf_reader = KubernetesPodOperator(
                    namespace=os.getenv('K8_NAMESPACE'),
                    image=naf_reader_image,
                    cmds=naf_reader_args,
                    name='naf_reader.{}'.format(country_config['name']),
                    task_id="naf-reader-task",
                    get_logs=True,
                    in_cluster=True if os.getenv('KUBERNETES_SERVICE_HOST') else False,
                    dag=dag
                )

                dag >> naf_reader >> source_exists

                dags.append(dag)

        return dags

naf_reader_daily_dag = NAFReaderDagFactory().build(dag_id='{}_daily'.format(PIPELINE))
