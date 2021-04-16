from __future__ import print_function
import requests
from jproperties import Properties
import json

import time
import generic_camunda_client
from generic_camunda_client.rest import ApiException
from pprint import pprint
import random


def get_configs(file_name):
    configs = Properties()
    with open(file_name, 'rb') as config_file:
        configs.load(config_file)
    items_view = configs.items()
    configs_dict = {}

    for item in items_view:
        configs_dict[item[0]] = item[1].data
    return configs_dict

def draw_city():
    cities = {0: 'Helsinki', 1: 'Aijala', 2: 'Frankfurt am Main'}
    ix = random.randint(0, 2)
    return cities.get(ix)

def run_get_city():
    config_dict = get_configs('CamundaAPIConfig.properties')
    fetch_and_lock_payload = {"workerId": "getCityWorker",
                              "maxTasks": 1,
                              "usePriority": "true",
                              "topics":
                                  [{"topicName": "DrawCity",
                                    "lockDuration": 30000
                                    }
                                   ]
                              }

    # Defining the host is optional and defaults to http://localhost:8080/engine-rest
    # See configuration.py for a list of all supported configuration parameters.
    host = config_dict.get('BaseURL')
    configuration = generic_camunda_client.Configuration(host)

    # Enter a context with an instance of the API client
    with generic_camunda_client.ApiClient(configuration) as api_client:
        api_instance = generic_camunda_client.ExternalTaskApi(api_client)

        try:
            # api_response = api_instance.evaluate_condition(evaluation_condition_dto=evaluation_condition_dto)
            api_response = api_instance.fetch_and_lock(fetch_external_tasks_dto=fetch_and_lock_payload)
            while not api_response:
                 time.sleep(5)
                 api_response = api_instance.fetch_and_lock(fetch_external_tasks_dto=fetch_and_lock_payload)
                 print('Fetch and lock response: ', api_response)

                 if api_response:
                     break
            task_id = api_response[0].id
        except ApiException as e:
            print("Exception when calling ExternalTaskApi->fetch_and_lock: %s\n" % e)

        try:
            city = draw_city()
            print(city)
            complete_external_task_dto = {"workerId": "getCityWorker",
                                          "variables": {"cityName": {"value": city}}}  # CompleteExternalTaskDto |  (optional)
            api_response = api_instance.complete_external_task_resource(task_id, complete_external_task_dto=complete_external_task_dto)
        except ApiException as e:
            print("Exception when calling ExternalTaskApi->complete_external_task_resource: %s\n" % e)


if __name__ == '__main__':
    try:
        while True:
            run_get_city()
            time.sleep(15)

    except KeyboardInterrupt:
        pass
