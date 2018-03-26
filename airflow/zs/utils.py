# -*- coding: utf-8 -*-
#
import os


def parse_project_and_task_from_dag_id(dag_id):
    """Parse project and task from dag id.

    Args:
        dag_id (str): The id of DAG.

    Returns:
        (tuple of str): The first item is project. The second item is task.
            If dag_id is invalid, will return empty string.
    """
    if not dag_id:
        return '', ''

    ids = dag_id.split('__')
    if len(ids) >= 3:
        return ids[1], ids[2]
    elif len(ids) == 2:
        return ids[1], ''
    else:
        return '', ''


def get_default_operator_env(dag_id):
    """Get default environment for operator.

    Args:
        dag_id (str): The id of DAG.

    Returns:
        (dict): Environment.
    """
    project, task = parse_project_and_task_from_dag_id(dag_id)
    return {
        'ZS_ENV': os.getenv('ZS_ENV', 'dev'),
        'ZS_LOG_LEVEL': os.getenv('ZS_LOG_LEVEL', 'INFO'),
        'ZS_FLUENTD_HOST': os.getenv('ZS_FLUENTD_HOST', 'localhost'),
        'ZS_FLUENTD_PORT': os.getenv('ZS_FLUENTD_PORT', '24224'),
        'ZS_PROJECT': project,
        'ZS_TASK': task,
        'ZS_SUBTASK': '{{ ti.task_id }}',
        'ZS_JOB': '{{ run_id }}',
    }
