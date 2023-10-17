
from dynaconf import Dynaconf

# reading .env file
CONFIG = Dynaconf(
    # envvar_prefix="DYNACONF",  # export envvars with `export DYNACONF_FOO=bar`.
    load_dotenv=True,
    envvar_prefix=False,
    # settings_files=['.env'],  # Load files in the given order.
)
