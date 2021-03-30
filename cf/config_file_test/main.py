import configparser

def test_config_file(event, context):

    cfg = configparser.ConfigParser()
    cfg.read("../../cha_pipeline.cfg")
    print(cfg["auth_setup"]["iam_key"])