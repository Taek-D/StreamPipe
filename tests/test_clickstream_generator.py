from src.simulator.clickstream_generator import EVENT_TYPES, PAGES, generate_event


def test_generate_event_returns_expected_shape() -> None:
    event = generate_event()

    assert event.event_type in EVENT_TYPES
    assert event.page in PAGES
    assert event.event_id
    assert event.user_id.startswith("user-")
    assert event.session_id
    assert event.timestamp
