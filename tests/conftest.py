import pytest

from etl_framework.local import local_spark_session


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = str(tmp_path_factory.mktemp("spark-warehouse"))
    session = local_spark_session(warehouse, app_name="etl-framework-tests")
    yield session
    session.stop()


@pytest.fixture()
def customers_df(spark):
    return spark.createDataFrame(
        [
            (1, "Alice", "alice@example.com"),
            (2, "Bob", "bob@example.com"),
            (3, "Carol", "carol@example.com"),
        ],
        "customer_id int, customer_name string, email string",
    )


@pytest.fixture()
def orders_df(spark):
    return spark.createDataFrame(
        [
            (100, 1, "2026-07-07", 50.0),
            (101, 1, "2026-07-06", 75.0),
            (102, 2, "2026-07-07", 20.0),
            (103, 9, "2026-07-07", 99.0),
        ],
        "order_id int, customer_id int, order_date string, order_amount double",
    )
