
from dynaconf import Dynaconf

settings = Dynaconf(
    envvar_prefix="KSINK",
    settings_files=['default.settings.toml', 'dev.settings.toml'],
)
