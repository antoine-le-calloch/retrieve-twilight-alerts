import os
import ast
import yaml
from pathlib import Path
from collections.abc import Mapping

_CONFIG = None

def _recursive_update(base, updates):
    for k, v in updates.items():
        if isinstance(v, Mapping) and isinstance(base.get(k), Mapping):
            base[k] = _recursive_update(base[k], v)
        else:
            base[k] = v
    return base


def _load_yaml(path):
    if path.exists():
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}
    return {}


def _apply_env_overrides(config):
    """Override config keys with ENV vars like SECTION_KEY=value"""
    for key_path, value in os.environ.items():
        keys = key_path.lower().split("_")
        ref = config
        for k in keys[:-1]:
            if k in ref and isinstance(ref[k], dict):
                ref = ref[k]
            else:
                break
        else:
            last_key = keys[-1]
            try:
                ref[last_key] = ast.literal_eval(value)
            except Exception:
                ref[last_key] = value
    return config


def load_config(config_path="config.yaml", defaults_path="config.defaults.yaml", reload=False):
    global _CONFIG
    if _CONFIG is not None and not reload:
        return _CONFIG

    defaults = _load_yaml(Path(defaults_path))
    overrides = _load_yaml(Path(config_path))
    config = _recursive_update(defaults, overrides)
    config = _apply_env_overrides(config)

    _CONFIG = config
    return config


class ConfigWrapper:
    """Allow cfg['a.b.c'] style access"""

    def __init__(self, data):
        self._data = data

    def __getitem__(self, key):
        ref = self._data
        for part in key.split("."):
            ref = ref[part]
        return ref

    def get(self, key, default=None, /):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def show(self):
        import pprint
        pprint.pprint(self._data)


def get_config(reload=False):
    return ConfigWrapper(load_config(reload=reload))
