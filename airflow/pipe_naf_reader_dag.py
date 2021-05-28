from airflow import DAG
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory

from datetime import datetime, timedelta


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

        self.config['source_gcs_path'] = self.country['gcs_source']
        config = self.config
        name = self.country['name']
        dag_id=self.get_dag_id_by_country(dag_id, name)

        one_day_before = datetime.now() - timedelta(days=1)
        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args, end_date=one_day_before) as dag:

            naf_reader = self.build_docker_task({
                'task_id':'naf-reader-task',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'naf-reader-{}'.format(name),
                'dag':dag,
                'arguments':['naf_reader_daily',
                             '{name}'.format(**self.country),
                             '{gcs_source}'.format(**self.country),
                             '{gcs_csv_output}'.format(**self.country),
                             '{bq_output}'.format(**self.country),
                             '{ds}'.format(**config),
                             '{schema_file_name}'.format(**self.country)]
            })

            generate_partitioned_table = self.build_docker_task({
                'task_id':'generate-partitioned-table-task',
                'pool':'k8operators_limit',
                'docker_run':'{docker_run}'.format(**config),
                'image':'{docker_image}'.format(**config),
                'name':'naf-reader-generate-partitioned-table-{}'.format(name),
                'dag':dag,
                'arguments':['generate_partitioned_table_daily',
                             '{name}'.format(**self.country),
                             '{bq_output}'.format(**self.country),
                             '{bq_partitioned_output}'.format(**self.country),
                             '{ds}'.format(**config)]
            })

            threshold_to_achieve = BigQueryCheckOperator(
                task_id='threshold_to_achieve',
                sql='SELECT if( count(*) < {threshold}, 0, count(*) ) FROM `{bq_partitioned_output}`'.format(**self.country),
                use_legacy_sql=False,
                retries=3,
                retry_delay=timedelta(minutes=30),
                max_retry_delay=timedelta(minutes=30),
                on_failure_callback=config_tools.failure_callback_gfw
            )

            for sensor in self.source_gcs_sensors(dag):
                dag >> sensor >> naf_reader >> generate_partitioned_table >> threshold_to_achieve

            return dag

country_configurations = config_tools.load_config(PIPELINE)['configurations']
for country_config in country_configurations:
    reader = NAFReaderDagFactory(country_config)
    dag_id = f'{PIPELINE}_daily'
    globals()[reader.get_dag_id_by_country(dag_id, country_config['name'])]= reader.build(dag_id=dag_id)
