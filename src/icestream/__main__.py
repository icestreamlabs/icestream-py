import asyncio
import functools
import logging
import signal

from hypercorn.asyncio import serve as hypercorn_serve
from hypercorn.config import Config as HypercornConfig

from icestream.admin import AdminApi
from icestream.compaction import CompactorWorker
from icestream.compaction.wal_to_parquet import WalToParquetProcessor
from icestream.compaction.wal_to_topic_wal import WalToTopicWalProcessor
from icestream.config import Config
from icestream.db import run_migrations
from icestream.kafkaserver.server import Server
from icestream.kafkaserver.internal_topics import ensure_internal_topics
from icestream.kafkaserver.producer_state import run_producer_state_reaper
from icestream.kafkaserver.types import ProduceTopicPartitionData
from icestream.kafkaserver.wal.manager import WALManager
from icestream.kafkaserver.consumer_group_liveness import run_consumer_group_reaper
from icestream.logger import log


async def run():
    shutdown_event = asyncio.Event()
    server_handle = None
    api_handle = None
    wal_manager_handle = None
    compaction_worker_handle = None
    group_reaper_handle = None
    producer_state_reaper_handle = None

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

        log.info("Ensuring internal topics...")
        await ensure_internal_topics(config)

        queue = asyncio.Queue[ProduceTopicPartitionData](maxsize=2000)
        wal_manager = WALManager(config, queue)
        wal_manager_handle = asyncio.create_task(wal_manager.run())

        server = Server(config=config, queue=queue)
        server_handle = asyncio.create_task(server.run(port=config.PORT))
        group_reaper_handle = asyncio.create_task(
            run_consumer_group_reaper(config, shutdown_event)
        )
        producer_state_reaper_handle = asyncio.create_task(
            run_producer_state_reaper(config, shutdown_event)
        )

        if config.ADMIN_API_ENABLED:
            admin_api = AdminApi(config)
            hypercorn_config = HypercornConfig()
            hypercorn_config.bind = [f"0.0.0.0:{config.ADMIN_PORT}"]
            hypercorn_config.use_reloader = False
            hypercorn_config.loglevel = "info"
            logging.getLogger("hypercorn.error").propagate = False

            api_handle = asyncio.create_task(
                hypercorn_serve(admin_api.app, hypercorn_config)
            )
        else:
            log.info("admin api disabled")

        compaction_processors = []
        if config.COMPACTION_FORMAT == "topic_wal":
            log.info("compaction format selected", format="topic_wal")
            compaction_processors = [WalToTopicWalProcessor()]
        elif config.COMPACTION_FORMAT == "parquet":
            log.warning(
                "legacy compaction format selected",
                format="parquet",
                note="active read paths use topic_wal by default",
            )
            compaction_processors = [WalToParquetProcessor()]
        elif config.COMPACTION_FORMAT == "none":
            log.info("compaction format selected", format="none")

        if config.ENABLE_COMPACTION and compaction_processors:
            compaction_worker = CompactorWorker(config, compaction_processors)
            compaction_worker_handle = asyncio.create_task(compaction_worker.run())

        wait_handles = [
            server_handle,
            wal_manager_handle,
            group_reaper_handle,
            producer_state_reaper_handle,
            asyncio.create_task(shutdown_event.wait()),
        ]
        if api_handle:
            wait_handles.append(api_handle)
        if config.ENABLE_COMPACTION and compaction_worker_handle:
            wait_handles.append(compaction_worker_handle)

        done, pending = await asyncio.wait(wait_handles, return_when=asyncio.FIRST_COMPLETED)

        if not shutdown_event.is_set():
            log.warning("task finished unexpectedly, initiating shutdown")
            shutdown_event.set()

    except asyncio.CancelledError:
        log.info("Received asyncio.CancelledError, shutting down gracefully...")
    except Exception as e:
        log.exception(f"error during server setup or runtime {e}")
    finally:
        log.info("shutting down")
        handles = [
            server_handle,
            api_handle,
            wal_manager_handle,
            group_reaper_handle,
            producer_state_reaper_handle,
        ]
        if config.ENABLE_COMPACTION and compaction_worker_handle:
            handles.append(compaction_worker_handle)
        for task in handles:
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
