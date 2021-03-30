import configparser

def test_config_file(event, context):

    cfg = configparser.RawConfigParser()
    cfg.read("../test_config.txt")
    print(cfg["auth_setup"]["iam_key"])