import asyncio
import functools
import signal

from icestream.config import Config
from icestream.db import run_migrations
from icestream.kafkaserver.server import Server
from icestream.logger import log

from icestream.admin import AdminApi

from hypercorn.config import Config as HypercornConfig
from hypercorn.asyncio import serve as hypercorn_serve


async def run():
    shutdown_event = asyncio.Event()
    server_handle = None
    api_handle = None

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

        server = Server(config=config)
        server_handle = asyncio.create_task(server.run())

        admin_api = AdminApi(config)
        hypercorn_config = HypercornConfig()
        hypercorn_config.bind = ["0.0.0.0:8080"]
        hypercorn_config.use_reloader = False
        hypercorn_config.loglevel = "info"

        api_handle = asyncio.create_task(hypercorn_serve(admin_api.app, hypercorn_config))
        done, pending = await asyncio.wait(
            [server_handle, api_handle, asyncio.create_task(shutdown_event.wait())],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if not shutdown_event.is_set():
            log.warning("task finished unexpectedly, initiating shutdown")
            shutdown_event.set()

    except asyncio.CancelledError:
        log.info("Received asyncio.CancelledError, shutting down gracefully...")
    except Exception:
        log.exception("error during server setup or runtime")
    finally:
        log.info("shutting down")
        for task in [server_handle, api_handle]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    log.info("task cancelled")
                except Exception:
                    log.exception("error during task shutdown")
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
