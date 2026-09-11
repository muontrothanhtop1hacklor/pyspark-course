"""Run Bronze ingestion for the web source."""

from lakehouse_batch_common import run_bronze


if __name__ == "__main__":
    run_bronze("web")
