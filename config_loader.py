# config_loader.py
import configparser

def load_config(path="config.ini"):
    cfg = configparser.ConfigParser()
    cfg.read(path)
    return cfg
