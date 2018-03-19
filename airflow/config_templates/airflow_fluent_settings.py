# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os

import py_logging
from airflow import configuration as conf
from fluent import asynchandler

LOG_LEVEL = conf.get('core', 'LOGGING_LEVEL').upper()
DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'airflow.config_templates.airflow_fluent_settings.FluentHandler',
        },
        'file.task': {
            'class': 'airflow.config_templates.airflow_fluent_settings.FluentHandler',
        },
        'file.processor': {
            'class': 'airflow.config_templates.airflow_fluent_settings.FluentHandler',
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': LOG_LEVEL
        },
        'airflow': {
            'handlers': ['console'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'airflow.processor': {
            'handlers': ['file.processor'],
            'level': LOG_LEVEL,
            'propagate': True,
        },
        'airflow.task': {
            'handlers': ['file.task'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'airflow.task_runner': {
            'handlers': ['file.task'],
            'level': LOG_LEVEL,
            'propagate': True,
        },
    }
}


class FluentHandler(logging.Handler):
    def __init__(self):
        super(FluentHandler, self).__init__()
        self.handler = None
        self.env = os.environ

    def set_context(self, ti):
        """ti task instance
        """
        host = self.env.get('FLUENTD_HOST')
        port = self.env.get('FLUENTD_PORT')
        job = None
        dr = ti.get_dagrun()
        if dr:
            job = dr.run_id

        project = self._get_project_id(ti.dag_id)
        kwargs = {'project': project, 'task': ti.dag_id,
                  'subtask': ti.task_id, 'job': job}

        fd_handler = asynchandler.FluentHandler(
            '%s.%s' % (self.env, project), host=host, port=port,
        )
        fd_handler.setLevel(self.level)
        fd_handler.setFormatter(py_logging.JSONFormatter(**kwargs))
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

    def _get_project_id(self, dag_id):
        ids = dag_id.split('__')
        if len(ids) >= 2 and ids[-1] != '':
            return ids[1]
