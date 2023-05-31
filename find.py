import re
import os
# list of different types of file

# rootdir = "dags"
# regex = re.compile('(.[0-9]*py)') 
# x = re.search("\s", txt)

# for root, dirs, files in os.walk(rootdir):
#     for file in files:
#     # search given pattern in the line 
#         if regex.match(file):
#             print(file)


# def getFileName(filename):
#         src = "{dag_name}_[0-9]+.py".format(dag_name=filename)
#         path = "dags"
#         dir_list = os.listdir(path)
#         for file in dir_list:
#             if bool(re.match(src, file)):
#                 return file
            
# print(getFileName('task_3'))


# import hocon
# import json
# from pyhocon import ConfigFactory, HOCONConverter, ConfigTree

# import jsoncfg
# # Load the JSON datacd
# # with open('dags/configure/abc.conf', 'r') as json_file:
# #     # json_data = json.load(json_file)
# #     data = jsoncfg.load_config(json_file)

# data = jsoncfg.load_config("dags/configure/abc.json")
# print(data)
# # Convert JSON to HOCON
# # hocon_data = HOCONConverter.convert(json_data,"hocon")

# # Save the HOCON data to a file
# with open('dags/configure/bash_command_2.conf', 'w') as hocon_file:
#     hocon_file.write(str(data))


# from python_json_config import ConfigBuilder

# # create config parser
# builder = ConfigBuilder()

# # parse config
# config = builder.parse_config('dags/configure/bash_command_2.json')
# with open('dags/configure/bash_command_2.conf', 'w') as hocon_file:
#     hocon_file.write(str(config))

# from __future__ import annotations
import attr

@attr.frozen
class Configuration:
    @attr.frozen
    class Files:
        input_dir: str
        output_dir: str
    files: Files
    @attr.frozen
    class Parameters:
        patterns: list[str]
    parameters: Parameters

def configuration_from_dict(details):
    files = Configuration.Files(
        input_dir=details["files"]["input-dir"],
        output_dir=details["files"]["output-dir"],
    )
    parameters = Configuration.Parameters(
        patterns=details["parameters"]["patterns"]
    )
    return Configuration(
        files=files,
        parameters=parameters,
    )

json_config = """
{
    "files": {
        "input-dir": "inputs",
        "output-dir": "outputs"
    },
    "parameters": {
        "patterns": [
            "*.txt",
            "*.md"
        ]
    }
}
"""
import json
def configuration_from_json(data):
    parsed = json.loads(data)
    return configuration_from_dict(parsed)

print(configuration_from_json(json_config))