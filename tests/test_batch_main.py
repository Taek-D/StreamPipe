from src.batch.main import (
    DROPOFF_LOCATION_ID_CANDIDATES,
    PICKUP_LOCATION_ID_CANDIDATES,
    get_payment_type_label,
    resolve_first_available_column,
)


def test_resolve_first_available_column_returns_matching_name() -> None:
    columns = ["VendorID", "PULocationID", "fare_amount"]

    assert resolve_first_available_column(columns, PICKUP_LOCATION_ID_CANDIDATES) == "PULocationID"
    assert resolve_first_available_column(columns, DROPOFF_LOCATION_ID_CANDIDATES) is None


def test_get_payment_type_label_maps_known_codes() -> None:
    assert get_payment_type_label(1) == "credit_card"
    assert get_payment_type_label("2") == "cash"
    assert get_payment_type_label(6) == "voided_trip"


def test_get_payment_type_label_defaults_to_other() -> None:
    assert get_payment_type_label(None) == "other"
    assert get_payment_type_label("99") == "other"
