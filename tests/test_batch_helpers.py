from src.batch.main import (
    DROPOFF_LOCATION_ID_CANDIDATES,
    PICKUP_LOCATION_ID_CANDIDATES,
    PICKUP_TIMESTAMP_CANDIDATES,
    get_payment_type_label,
    resolve_first_available_column,
)


def test_resolve_first_available_column_returns_first_match() -> None:
    columns = ["VendorID", "PULocationID", "DOLocationID", "tpep_pickup_datetime"]

    assert resolve_first_available_column(columns, PICKUP_TIMESTAMP_CANDIDATES) == "tpep_pickup_datetime"
    assert resolve_first_available_column(columns, PICKUP_LOCATION_ID_CANDIDATES) == "PULocationID"
    assert resolve_first_available_column(columns, DROPOFF_LOCATION_ID_CANDIDATES) == "DOLocationID"


def test_resolve_first_available_column_returns_none_when_missing() -> None:
    columns = ["VendorID", "pickup_datetime_text"]

    assert resolve_first_available_column(columns, PICKUP_TIMESTAMP_CANDIDATES) is None


def test_get_payment_type_label_maps_known_and_unknown_values() -> None:
    assert get_payment_type_label(1) == "credit_card"
    assert get_payment_type_label("2") == "cash"
    assert get_payment_type_label(99) == "other"
    assert get_payment_type_label(None) == "other"
