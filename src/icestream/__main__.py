import asyncio
import functools
import signal

from icestream.config import Config
from icestream.db import run_migrations
from icestream.kafkaserver.server import Server
from icestream.logger import log


async def run():
    shutdown_event = asyncio.Event()
    server_handle = None

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
        done, pending = await asyncio.wait(
            [server_handle, asyncio.create_task(shutdown_event.wait())],
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not shutdown_event.is_set():
            log.warning("task finished unexpectedly, initiating shutdown")
            shutdown_event.set()
    except Exception:
        log.exception("error during server setup or runtime")
    finally:
        log.info("shutting down")
        if server_handle and not server_handle.done():
            server_handle.cancel()
            try:
                await server_handle
            except asyncio.CancelledError:
                log.info("server task cancelled")
            except Exception:
                log.exception("error during server task shutdown")
        log.info("shutdown complete")


def main():
    log.info("Starting icestream")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("keyboard interrupt caught, exiting")


if __name__ == "__main__":
    main()
