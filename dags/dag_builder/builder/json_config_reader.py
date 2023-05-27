import json


class JsonConfigReader:
    @staticmethod
    def read(file_name):
        with open(file_name) as f:
            json_string = f.read()
        return json.loads(json_string)

    @staticmethod
    def get_property(config, path, default=None):
        properties = path.split(".")
        current_property = config
        current_path = []
        for i in range(len(properties)):
            current_path.append(properties[i])
            try:
                current_property = current_property[properties[i]]
            except KeyError:
                # logging.warning("Key not found {current_path} in config {config}"
                #                 .format(current_path=".".join(current_path), config=config))
                return default
        return current_property
