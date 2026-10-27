"""Create the local Spark session used by every layer.

On Windows, Spark is started through .cmd scripts that break on paths with spaces and on
old environment variables. prepare_environment() fixes that before Spark starts:
  * PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON -> always this Python (your .venv), never an old value
  * SPARK_HOME -> the pyspark package that is installed in this .venv (ignores an old SPARK_HOME)
  * JAVA_HOME  -> checked; on Windows converted to its short 8.3 form (no spaces, e.g. C:\\PROGRA~1\\...)
"""
import os
import shutil
import sys
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

import config

POSTGRES_JDBC_JAR = "org.postgresql:postgresql:42.7.4"


def _short_path(path: str) -> str:
    """Windows only: 'C:\\Program Files\\x' -> 'C:\\PROGRA~1\\x' (Spark's .cmd scripts dislike spaces)."""
    if os.name != "nt" or " " not in path:
        return path
    import ctypes
    buffer = ctypes.create_unicode_buffer(1024)
    if ctypes.windll.kernel32.GetShortPathNameW(path, buffer, 1024):
        return buffer.value
    return path


def _find_java_home() -> str:
    """JAVA_HOME if it is valid, otherwise derived from the 'java' found on PATH."""
    java_exe = "java.exe" if os.name == "nt" else "java"
    home = os.environ.get("JAVA_HOME", "").strip().strip('"').rstrip("\\/")
    if home.lower().endswith(("\\bin", "/bin")):                 # common mistake: JAVA_HOME = ...\bin
        home = home[:-4]
    if home and (Path(home) / "bin" / java_exe).exists():
        return home
    on_path = shutil.which("java")
    if on_path:
        return str(Path(on_path).resolve().parent.parent)
    raise RuntimeError(
        "Java not found. Install Java 17 or 21 (Microsoft Build of OpenJDK) and set JAVA_HOME to the JDK folder "
        "(the folder that contains 'bin\\java.exe'), then restart VS Code. "
        f"Current JAVA_HOME = '{os.environ.get('JAVA_HOME', '')}'")


def prepare_environment() -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable                 # always this .venv's Python
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["SPARK_HOME"] = _short_path(str(Path(pyspark.__file__).parent))
    os.environ["JAVA_HOME"] = _short_path(_find_java_home())


def get_spark(app_name: str = "datatrust_medallion_poc") -> SparkSession:
    prepare_environment()
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")                                       # use all CPU cores of the laptop
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")       # same as PostgreSQL TIMESTAMP: no time shifts
        .config("spark.sql.shuffle.partitions", "4")              # small data: fewer, faster tasks
        .config("spark.ui.showConsoleProgress", "false")
    )
    if config.WRITE_MODE == "jdbc":
        builder = builder.config("spark.jars.packages", POSTGRES_JDBC_JAR)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


if __name__ == "__main__":                                        # quick check: python spark_utils.py
    prepare_environment()
    for key in ("PYSPARK_PYTHON", "SPARK_HOME", "JAVA_HOME"):
        print(f"{key:<15} {os.environ[key]}")
    s = get_spark("check")
    s.range(3).show()
    print("Spark", s.version, "is working")
    s.stop()
    