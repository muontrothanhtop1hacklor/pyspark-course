"""Run Bronze ingestion for the store source."""

from lakehouse_batch_common import run_bronze


if __name__ == "__main__":
    run_bronze("store")
