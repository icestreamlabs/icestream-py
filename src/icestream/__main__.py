import asyncio
import functools
import logging
import signal

from hypercorn.asyncio import serve as hypercorn_serve
from hypercorn.config import Config as HypercornConfig

from icestream.admin import AdminApi
from icestream.config import Config
from icestream.db import run_migrations
from icestream.kafkaserver.server import Server
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.kafkaserver.wal.manager import WALManager
from icestream.logger import log


async def run():
    shutdown_event = asyncio.Event()
    server_handle = None
    api_handle = None
    wal_manager_handle = None

    def _signal_handler(*_):
        log.warning("shutdown signal received")
        shutdown_event.set()

    loop = asyncio.get_event_loop()

    # satisfy the pycharm type checker
    functools.partial(loop.add_signal_handler, signal.SIGINT, _signal_handler)()
    functools.partial(loop.add_signal_handler, signal.SIGTERM, _signal_handler)()
    config = Config()

    try:
        log.info("Running DB migrations...")
        await run_migrations(config)
        log.info("Migrations complete.")

        queue = asyncio.Queue[ProduceTopicPartitionData](maxsize=2000)
        wal_manager = WALManager(config, queue)
        wal_manager_handle = asyncio.create_task(wal_manager.run())

        server = Server(config=config, queue=queue)
        server_handle = asyncio.create_task(server.run())

        admin_api = AdminApi(config)
        hypercorn_config = HypercornConfig()
        hypercorn_config.bind = ["0.0.0.0:8080"]
        hypercorn_config.use_reloader = False
        hypercorn_config.loglevel = "info"
        logging.getLogger("hypercorn.error").propagate = False

        api_handle = asyncio.create_task(
            hypercorn_serve(admin_api.app, hypercorn_config)
        )
        done, pending = await asyncio.wait(
            [
                server_handle,
                api_handle,
                wal_manager_handle,
                asyncio.create_task(shutdown_event.wait()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if not shutdown_event.is_set():
            log.warning("task finished unexpectedly, initiating shutdown")
            shutdown_event.set()

    except asyncio.CancelledError:
        log.info("Received asyncio.CancelledError, shutting down gracefully...")
    except Exception as e:
        log.exception(f"error during server setup or runtime {e}")
    finally:
        log.info("shutting down")
        for task in [server_handle, api_handle, wal_manager_handle]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    log.info("task cancelled")
                except Exception as e:
                    log.exception(f"error during task shutdown {e}")
        log.info("shutdown complete")


def main():
    log.info("Starting icestream")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("keyboard interrupt caught, exiting")
    except asyncio.CancelledError:
        log.info("asyncio.CancelledError caught after shutdown, exiting cleanly")


if __name__ == "__main__":
    main()
