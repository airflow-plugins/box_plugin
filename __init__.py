from airflow.plugins_manager import AirflowPlugin
from BoxPlugin.hooks.box_hook import BoxHook

class BoxPlugin(AirflowPlugin):
    name = "box_plugin"
    operators = []
    hooks = [BoxHook]
