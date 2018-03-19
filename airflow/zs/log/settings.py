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


from airflow import configuration as conf

LOG_LEVEL = conf.get('core', 'LOGGING_LEVEL').upper()

DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'text': {
            'class': 'airflow.zs.log.handler.TextFluentHandler',
        },
        'task': {
            'class': 'airflow.zs.log.handler.TaskFluentHandler',
        },
    },
    'loggers': {
        '': {
            'handlers': ['text'],
            'level': LOG_LEVEL
        },
        'airflow': {
            'handlers': ['text'],
            'level': LOG_LEVEL,
            'propagate': False,
        },
        'airflow.task': {
            'handlers': ['task'],
            'level': 'INFO',
            'propagate': False,
        },
        'airflow.task_runner': {
            'handlers': ['task'],
            'level': 'INFO',
            'propagate': False,
        },
        'airflow.operators': {
            'handlers': ['task'],
            'level': 'INFO',
            'propagate': False,
        },

    }
}
