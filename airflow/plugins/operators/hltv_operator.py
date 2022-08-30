import json
from datetime import datetime
from pathlib import Path
from os.path import join

from airflow.models import BaseOperator, DAG, TaskInstance
from airflow.utils.decorators import apply_defaults
from hooks.hltv_hook import HltvHook


class HltvOperator(BaseOperator):

    template_fields = [
        'file_path'
    ]

    def __init__(
        self,
        file_path,
        conn_id = None, 
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.conn_id = conn_id

    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(
            parents = True,
            exist_ok = True
        )

    def execute(self, context):
        hook = HltvHook(
            execution_date = context['ds'],
        )
        self.create_parent_folder()

        with open(self.file_path, 'w') as output_file:
            result = hook.run()
            json.dump(result, output_file, ensure_ascii=False)

if __name__ == '__main__':
    with DAG(dag_id='HltvTest', start_date=datetime.now()) as dag:
        to = HltvOperator(
            file_path=join(
                '/home/lucas/pipelines/raw',
                'hltv',
                'extract_date={{ ds }}',
                'Hltv_{{ ds_nodash }}.json'
                ),
            task_id='test_run',
            dag=dag
        )
        #ti = TaskInstance(task=to, execution_date=datetime.now())
        to.execute(None)