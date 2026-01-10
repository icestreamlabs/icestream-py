import asyncio
import os

from alembic import command
from alembic.config import Config
from sqlalchemy.engine.url import make_url

from icestream.config import Config as IcestreamConfig


async def run_migrations(icestream_config: IcestreamConfig):
    url = make_url(icestream_config.DATABASE_URL)
    if not url.drivername.startswith("postgresql"):
        raise ValueError(f"Unsupported database dialect: {url.drivername}")

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
    alembic_ini_path = os.path.join(base_dir, "alembic.ini")

    alembic_cfg = Config(alembic_ini_path)
    script_location = os.path.join(base_dir, "src", "icestream", "alembic")
    alembic_cfg.set_main_option("script_location", script_location)

    await asyncio.to_thread(command.upgrade, alembic_cfg, "head")
