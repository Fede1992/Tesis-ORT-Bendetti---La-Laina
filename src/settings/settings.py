from typing import Any
from yaml import load
from yaml.loader import SafeLoader
import os

def load_settings(key: str | None = None) -> dict[str, Any]:
    """
    Loads the settings from the config.yml file.

    Args:
        key (str | None): Optional key to get a specific section
                          (e.g., "general", "source", "scrape", "export").

    Returns:
        dict[str, Any]: The settings or the requested section.
    """

    config_path = os.path.join("src", "settings", "config.yml")
    with open(config_path, "r", encoding="utf-8") as f:
        config = load(f, Loader=SafeLoader)

    if key:
        return config.get(key, {})

    return config

