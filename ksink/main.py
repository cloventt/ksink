import logging
import os
import platform
import signal
import sys

import ecs_logging

from config import settings
from ksink.app import Ksink

handler = logging.StreamHandler()
handler.setFormatter(ecs_logging.StdlibFormatter(
    extra={
        'host.hostname': platform.node(),
        'host.name': platform.node(),
        'host.architecture': platform.machine(),
        'service.name': 'ksink',
        'service.node.name': settings.app.instance_id,
        'service.version': os.environ.get('APP_VERSION', 'UNKNOWN')
    }
))
handler.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    handlers=[handler]
)


def terminate():
    logging.info("SIGTERM Received, shutting down...")
    sys.exit(0)


if __name__ == '__main__' and __package__ is None:
    signal.signal(signal.SIGTERM, terminate)

    try:
        with Ksink(settings) as app:
            app.run()
    except KeyboardInterrupt:
        logging.info('I have been asked nicely to die')
    except Exception as e:
        logging.critical('Encountered a fatal unhandled exception that crashed the app: %s', e, exc_info=e)
        raise e
    finally:
        logging.info('(x_x) I am now dead')
