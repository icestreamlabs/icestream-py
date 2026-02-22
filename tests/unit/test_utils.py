from icestream.utils import escape_topic_key_component


def test_escape_topic_key_component_passthrough_for_simple_topic():
    assert escape_topic_key_component("orders") == "orders"


def test_escape_topic_key_component_escapes_path_and_spaces():
    assert escape_topic_key_component("tenant/a events") == "tenant%2Fa%20events"


def test_escape_topic_key_component_is_deterministic():
    topic = "..//topic?name#x"
    first = escape_topic_key_component(topic)
    second = escape_topic_key_component(topic)
    assert first == second
    assert "/" not in first
