from airflow.plugins_manager import AirflowPlugin
from operators.hltv_operator import HltvOperator

class HltvAirflowPlugin(AirflowPlugin):
    name = "hltv"
    operators = [HltvOperator]