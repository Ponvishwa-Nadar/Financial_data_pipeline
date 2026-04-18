from loguru import logger
import sys
from pathlib import Path
BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)


logger.remove()


logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
           "<level>{level}</level> | "
           "<cyan>{name}:{line}</cyan> - "
           "<level>{message}</level>",
    level="INFO",
    colorize=True
)


logger.add(
    LOG_DIR / "pipeline_{time:YYYY-MM-DD}.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | "
           "{level} | "
           "{name}:{line} - "
           "{message}",
    level="INFO",
    colorize=False,
    rotation="1 day",
    retention="7 days",
    compression="zip"
)
