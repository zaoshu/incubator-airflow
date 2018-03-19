# -*- coding: utf-8 -*-
#

import logging
import os

import py_logging
from fluent import asynchandler


def _load_config_from_env():
    return {
        'env': os.getenv('ZS_ENV', 'dev'),
        'fluentd_host': os.getenv('ZS_FLUENTD_HOST', 'localhost'),
        'fluentd_port': int(os.getenv('ZS_FLUENTD_PORT', 24224)),
    }


def _get_fluent_handler(**kwargs):
    config = _load_config_from_env()
    fd_handler = asynchandler.FluentHandler(
        '%s.%s' % (config['env'], kwargs.get('project', 'airflow')),
        host=config['fluentd_host'], port=config['fluentd_port'],
    )
    fd_handler.setFormatter(py_logging.JSONFormatter(**kwargs))
    return fd_handler


class TaskFluentHandler(logging.Handler):

    def __init__(self):
        super(TaskFluentHandler, self).__init__()
        self.handler = None

    def set_context(self, ti):
        """ti task instance
        """
        job = None
        dr = ti.get_dagrun()
        if dr:
            job = dr.run_id

        project, task = self._parse_project_and_task(ti.dag_id)
        kwargs = {
            'project': project,
            'task': task,
            'subtask': ti.task_id,
            'job': job,
        }

        fd_handler = _get_fluent_handler(**kwargs)
        fd_handler.setLevel(self.level)
        self.handler = fd_handler

    def emit(self, record):
        if self.handler is not None:
            self.handler.emit(record)

    def flush(self):
        if self.handler is not None:
            self.handler.flush()

    def close(self):
        if self.handler is not None:
            self.handler.close()

    @staticmethod
    def _parse_project_and_task(dag_id):
        ids = dag_id.split('__')
        if len(ids) >= 3:
            return ids[1], ids[2]
        elif len(ids) == 2:
            return ids[1], ''
        else:
            return '', ''


class TextFluentHandler(logging.Handler):

    def __init__(self):
        super(TextFluentHandler, self).__init__()
        fd_handler = _get_fluent_handler()
        fd_handler.setLevel(self.level)
        self.handler = fd_handler

    def emit(self, record):
        if self.handler is not None:
            self.handler.emit(record)

    def flush(self):
        if self.handler is not None:
            self.handler.flush()

    def close(self):
        if self.handler is not None:
            self.handler.close()
