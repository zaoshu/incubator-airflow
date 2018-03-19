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
import sys

import py_logging
import requests
from airflow import configuration as conf
from airflow.configuration import AirflowConfigException
from airflow.utils.file import mkdirs
from fluent import asynchandler
from jinja2 import Template

LOG_LEVEL = conf.get('core', 'LOGGING_LEVEL').upper()
LOG_FORMAT = conf.get('core', 'log_format')

BASE_LOG_FOLDER = conf.get('core', 'BASE_LOG_FOLDER')
PROCESSOR_LOG_FOLDER = conf.get('scheduler', 'child_process_log_directory')

FILENAME_TEMPLATE = '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log'
PROCESSOR_FILENAME_TEMPLATE = '{{ filename }}.log'

DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': LOG_FORMAT,
        },
        'airflow.processor': {
            'format': LOG_FORMAT,
        },
        'custom': {
            'class': 'py_logging.JSONFormatter',
        }
    },
    'handlers': {
        'console': {
            'class': 'airflow.config_templates.airflow_fluent_settings.FluentHandler',
            # 'formatter': 'airflow.task',
            # 'stream': 'ext://sys.stdout'
        },
        'file.task': {
            'class': 'airflow.config_templates.airflow_fluent_settings.FluentHandler',
        },
        'file.processor': {
            'class': 'airflow.config_templates.airflow_fluent_settings.FluentHandler',
            # 'formatter': 'airflow.processor',
            'base_log_folder': os.path.expanduser(PROCESSOR_LOG_FOLDER),
            'filename_template': PROCESSOR_FILENAME_TEMPLATE,
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
        pro_id = self._get_project_id(ti.dag_id)
        fd_handler = asynchandler.FluentHandler(
            '%s.%s' % (self.env, pro_id), host=self.env.get('fluentd_host'), port=self.env.get('fluentd_port'),
        )
        fd_handler.setLevel(self.level)
        fd_handler.setFormatter(py_logging.JSONFormatter(project=pro_id))
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
