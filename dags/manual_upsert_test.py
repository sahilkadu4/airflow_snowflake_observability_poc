from datetime import datetime
import logging

from observability_callbacks import _upsert_dag_run

log = logging.getLogger(__name__)


class _Obj:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def main():
    now = datetime.utcnow()
    ctx = {
        "dag": _Obj(dag_id="manual_test_dag"),
        "dag_run": _Obj(
            run_id="manual_test_run_1",
            run_type="manual",
            execution_date=now,
            start_date=now,
            end_date=now,
        ),
        "dag_id": "manual_test_dag",
        "run_id": "manual_test_run_1",
        "execution_date": now,
    }

    print("Running manual upsert test to Snowflake...")
    _upsert_dag_run(ctx, "TEST")
    print("Done.")


if __name__ == "__main__":
    main()
