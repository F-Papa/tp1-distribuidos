import json
import os
import threading
import time

import pytest
from src.controller_state.controller_state import ControllerState
from src.controllers.proxy.main import Proxy
from src.messaging.message import Message
from test.mocks.mock_messaging import MockMessaging, ProvokedError


def test_proxy_dispatches_data_to_filters_based_on_conn_id():
    barrier_config = {"FILTER_COUNT": 2, "FILTER_TYPE": "test_filter"}

    conn_id_1 = 5
    conn_id_2 = 6
    controller_id = f"{barrier_config.get('FILTER_TYPE')}_proxy"
    file_path = f"test/{controller_id}.json"
    temp_file_path = f"test/{controller_id}.tmp"

    state = Proxy.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    messaging = MockMessaging(
        host="test",
        port=1234,
        sender_id=controller_id,
        times_to_listen=3,
    )

    data_1 = {
        "transaction_id": 1,
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    data_2 = {
        "transaction_id": 2,
        "conn_id": conn_id_2,
        "queries": [1],
        "data": [
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    data_3 = {
        "transaction_id": 3,
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    proxy = Proxy(barrier_config, messaging, state=state)  # type: ignore

    messaging.send_to_queue(
        proxy.input_queue(), Message(data_1), sender_id="another_filter"
    )
    messaging.send_to_queue(
        proxy.input_queue(), Message(data_2), sender_id="another_filter"
    )
    messaging.send_to_queue(
        proxy.input_queue(), Message(data_3), sender_id="another_filter"
    )

    threading.Thread(target=proxy.start).start()

    expected_fwd_1 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_fwd_2 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id_2,
        "queries": [1],
        "data": [
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_fwd_3 = {
        "sender": controller_id,
        "transaction_id": 2,
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    filter_1_queue, filter_2_queue = proxy.filter_queues()

    fwd_1 = messaging.get_msgs_from_queue(filter_1_queue)
    fwd_2 = messaging.get_msgs_from_queue(filter_2_queue)
    fwd_3 = messaging.get_msgs_from_queue(filter_1_queue)

    assert expected_fwd_1 == json.loads(fwd_1)
    assert expected_fwd_2 == json.loads(fwd_2)
    assert expected_fwd_3 == json.loads(fwd_3)

    time.sleep(0.1)

    assert len(messaging.queued_msgs[filter_1_queue]) == 0
    assert len(messaging.queued_msgs[filter_2_queue]) == 0

    time.sleep(0.1)

    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)

def test_proxy_dispatches_eof_to_all_filters():
    barrier_config = {"FILTER_COUNT": 2, "FILTER_TYPE": "test_filter"}

    conn_id = 5
    controller_id = f"{barrier_config.get('FILTER_TYPE')}_proxy"
    file_path = f"test/{controller_id}.json"
    temp_file_path = f"test/{controller_id}.tmp"

    messaging = MockMessaging(
        sender_id=controller_id,
        host="test",
        port=1234,
        times_to_listen=2,
    )

    data_1 = {
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    eof_1 = {
        "transaction_id": 2,
        "EOF": True,
        "conn_id": conn_id,
        "queries": [1],
    }

    state = Proxy.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    proxy = Proxy(barrier_config, messaging, state=state)  # type: ignore

    messaging.send_to_queue(
        proxy.input_queue(), Message(data_1), sender_id="another_filter"
    )
    messaging.send_to_queue(
        proxy.input_queue(), Message(eof_1), sender_id="another_filter"
    )

    threading.Thread(target=proxy.start).start()

    expected_data_filter_1 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_eof_filter_1 = {
        "sender": controller_id,
        "transaction_id": 2,
        "conn_id": conn_id,
        "queries": [1],
        "EOF": True,
    }

    expected_eof_filter_2 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [1],
        "EOF": True,
    }

    filter_1_queue, filter_2_queue = proxy.filter_queues()

    fwd_data_filter_1 = messaging.get_msgs_from_queue(filter_1_queue)
    fwd_eof_filter_1 = messaging.get_msgs_from_queue(filter_1_queue)
    fwd_eof_filter_2 = messaging.get_msgs_from_queue(filter_2_queue)

    assert expected_data_filter_1 == json.loads(fwd_data_filter_1)
    assert expected_eof_filter_1 == json.loads(fwd_eof_filter_1)
    assert expected_eof_filter_2 == json.loads(fwd_eof_filter_2)

    time.sleep(0.1)

    assert len(messaging.queued_msgs[filter_1_queue]) == 0
    assert len(messaging.queued_msgs[filter_2_queue]) == 0

    time.sleep(0.1)

    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)

def test_proxy_recovers_from_crash_distributing_data():
    barrier_config = {"FILTER_COUNT": 2, "FILTER_TYPE": "test_filter"}

    conn_id_1 = 5
    conn_id_2 = 6

    controller_id = f"{barrier_config.get('FILTER_TYPE')}_proxy"
    file_path = f"test/{controller_id}.json"
    temp_file_path = f"test/{controller_id}.tmp"

    state = Proxy.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    messaging = MockMessaging(
        sender_id=controller_id,
        host="test",
        port=1234,
        times_to_listen=5,
        crash_on_send=5,
    )

    data_1 = {
        "transaction_id": 1,
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    data_2 = {
        "transaction_id": 2,
        "conn_id": conn_id_2,
        "queries": [1],
        "data": [
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    data_3 = {
        "transaction_id": 3,
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    proxy = Proxy(barrier_config, messaging, state=state)  # type: ignore

    messaging.send_to_queue(
        proxy.input_queue(), Message(data_1), sender_id="another_filter"
    )
    messaging.send_to_queue(
        proxy.input_queue(), Message(data_2), sender_id="another_filter"
    )
    messaging.send_to_queue(
        proxy.input_queue(), Message(data_3), sender_id="another_filter"
    )

    try:
        threading.Thread(target=proxy.start).start()
    except ProvokedError:
        pass

    time.sleep(0.1)

    state = Proxy.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    if os.path.exists(state.file_path):
        state.update_from_file()

    proxy = Proxy(barrier_config, messaging, state=state)  # type: ignore

    threading.Thread(target=proxy.start).start()

    expected_fwd_1_filter_1 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_fwd_1_filter_2 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id_2,
        "queries": [1],
        "data": [
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_fwd_2_filter_1 = {
        "sender": controller_id,
        "transaction_id": 2,
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    filter_1_queue, filter_2_queue = proxy.filter_queues()

    fwd_1_filter_1 = messaging.get_msgs_from_queue(filter_1_queue)
    fwd_1_filter_2 = messaging.get_msgs_from_queue(filter_2_queue)
    fwd_2_filter_1 = messaging.get_msgs_from_queue(filter_1_queue)

    assert expected_fwd_1_filter_1 == json.loads(fwd_1_filter_1)
    assert expected_fwd_1_filter_2 == json.loads(fwd_1_filter_2)
    assert expected_fwd_2_filter_1 == json.loads(fwd_2_filter_1)

    time.sleep(0.1)

    assert len(messaging.queued_msgs[filter_1_queue]) == 0
    assert len(messaging.queued_msgs[filter_2_queue]) == 0

    time.sleep(0.1)

    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)

def test_proxy_recovers_from_crash_dispatching_eof_to_its_filters():
    barrier_config = {"FILTER_COUNT": 2, "FILTER_TYPE": "test_filter"}

    conn_id_1 = 5
    conn_id_2 = 6
    controller_id = f"{barrier_config.get('FILTER_TYPE')}_proxy"
    file_path = f"test/{controller_id}.json"
    temp_file_path = f"test/{controller_id}.tmp"

    state = Proxy.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    messaging = MockMessaging(
        sender_id=controller_id,
        host="test",
        port=1234,
        times_to_listen=2,
        crash_on_send=3,
    )

    eof_1 = {
        "transaction_id": 1,
        "EOF": True,
        "conn_id": conn_id_1,
        "queries": [1],
    }

    proxy = Proxy(barrier_config, messaging, state=state)  # type: ignore

    messaging.send_to_queue(
        proxy.input_queue(), Message(eof_1), sender_id="another_filter"
    )

    try:
        threading.Thread(target=proxy.start).start()
    except ProvokedError:
        pass

    time.sleep(0.1)

    state = Proxy.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    if os.path.exists(state.file_path):
        state.update_from_file()

    proxy = Proxy(barrier_config, messaging, state=state)  # type: ignore
    threading.Thread(target=proxy.start).start()

    expected_eof = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id_1,
        "queries": [1],
        "EOF": True,
    }

    filter_1_queue, filter_2_queue = proxy.filter_queues()

    fwd_eof1 = messaging.get_msgs_from_queue(filter_1_queue)
    fwd_eof1_dup = messaging.get_msgs_from_queue(filter_1_queue)
    fwd_eof2 = messaging.get_msgs_from_queue(filter_2_queue)

    assert expected_eof == json.loads(fwd_eof1)
    assert fwd_eof2 == fwd_eof1
    assert fwd_eof1_dup == fwd_eof1

    time.sleep(0.1)

    assert len(messaging.queued_msgs[filter_1_queue]) == 0
    assert len(messaging.queued_msgs[filter_2_queue]) == 0

    time.sleep(0.1)

    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
