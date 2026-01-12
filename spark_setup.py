from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from pyspark.sql import SparkSession

EXPECTED_MAJOR_MINOR = ("3", "4")
DEFAULT_SPARK_PACKAGES = [
    "io.projectglow:glow-spark3_2.12:2.0.0",
    "com.databricks:spark-xml_2.12:0.17.0",
]
DEFAULT_REPOSITORIES = (
    "https://repo1.maven.org/maven2",
    "https://repo.maven.apache.org/maven2",
    "https://repos.spark-packages.org",
)
LOCAL_JDK_RELATIVE = Path(".jdk/jdk-11.0.29+7/Contents/Home")
IVY_SETTINGS_FILENAME = "ivysettings.xml"


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _ensure_java_home(repo_root: Path) -> None:
    if os.environ.get("JAVA_HOME"):
        return
    jdk_home = repo_root / LOCAL_JDK_RELATIVE
    if jdk_home.is_dir():
        os.environ["JAVA_HOME"] = str(jdk_home)
        os.environ["PATH"] = f"{jdk_home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"


def _ensure_ivy_settings(repo_root: Path) -> Path:
    settings_path = repo_root / IVY_SETTINGS_FILENAME
    if settings_path.exists():
        return settings_path
    settings_path.write_text(
        "\n".join(
            [
                '<?xml version="1.0" encoding="UTF-8"?>',
                "<ivysettings>",
                '  <settings defaultResolver="main"/>',
                "  <resolvers>",
                '    <chain name="main" returnFirst="true">',
                '      <ibiblio name="central" m2compatible="true" '
                'root="https://repo.maven.apache.org/maven2"/>',
                '      <ibiblio name="repo1" m2compatible="true" '
                'root="https://repo1.maven.org/maven2"/>',
                '      <ibiblio name="spark-packages" m2compatible="true" '
                'root="https://repos.spark-packages.org"/>',
                "    </chain>",
                "  </resolvers>",
                "</ivysettings>",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return settings_path


def _check_spark_version(expected: Sequence[str]) -> None:
    import pyspark

    parts = pyspark.__version__.split(".")
    if tuple(parts[:2]) != tuple(expected):
        raise RuntimeError(
            f"Expected pyspark {'.'.join(expected)}.x, got {pyspark.__version__}"
        )


def get_spark_session(
    *,
    app_name: str = "glow-local",
    packages: Iterable[str] | None = None,
    expected_spark: Sequence[str] = EXPECTED_MAJOR_MINOR,
    extra_conf: Mapping[str, str] | None = None,
) -> SparkSession:
    repo_root = _repo_root()
    _ensure_java_home(repo_root)
    _check_spark_version(expected_spark)
    ivy_dir = repo_root / ".ivy2"
    ivy_dir.mkdir(exist_ok=True)
    ivy_settings = _ensure_ivy_settings(repo_root)

    resolved_packages = list(packages) if packages is not None else DEFAULT_SPARK_PACKAGES
    builder = SparkSession.builder.appName(app_name)
    if resolved_packages:
        builder = builder.config("spark.jars.packages", ",".join(resolved_packages))
    builder = (
        builder.config("spark.jars.repositories", ",".join(DEFAULT_REPOSITORIES))
        .config("spark.jars.ivy", str(ivy_dir))
        .config("spark.jars.ivySettings", str(ivy_settings))
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
    )
    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)
    return builder.getOrCreate()


def register_glow(spark: SparkSession) -> SparkSession:
    import glow

    return glow.register(spark)
