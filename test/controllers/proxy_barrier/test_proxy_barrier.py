import json
import os
import threading
import time

import pytest
from src.controller_state.controller_state import ControllerState
from src.controllers.proxy_barrier.main import ProxyBarrier
from src.messaging.message import Message
from test.mocks.mock_messaging import MockMessaging, ProvokedError

def test_proxy_barrier_forwards_data_in_rr():
    barrier_config = {"FILTER_COUNT": 2, "FILTER_TYPE": "test_filter"}

    conn_id = 5
    controller_id = f"{barrier_config.get('FILTER_TYPE')}_proxy_barrier"
    file_path = f"test/{controller_id}.json"
    temp_file_path = f"test/{controller_id}.tmp"

    extra_fields = {
        "type_last_message": ProxyBarrier.DATA,
        "proxy.EOF": False,
        "queries": [],
        "conn_id": 0,
        "proxy.current_queue": 0,
        "proxy.data": [],
        "barrier.eof_count": {},
        "barrier.forward_to": "",
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    # if os.path.exists(state.file_path):
    #     state.update_from_file()

    input_queue = "test_filter_queue"
    filter_1_queue = "test_filter1"
    filter_2_queue = "test_filter2"
    output_queue = "output_queue"

    messaging = MockMessaging(
        host="test",
        port=1234,
        queues_to_export=[filter_1_queue, filter_2_queue, output_queue],
        msgs_to_consume=3,
    )

    data_1 = {
        "transaction_id": "test#1",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    data_2 = {
        "transaction_id": "test#2",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    data_3 = {
        "transaction_id": "test#3",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    messaging.send_to_queue(input_queue, Message(data_1))
    messaging.send_to_queue(input_queue, Message(data_2))
    messaging.send_to_queue(input_queue, Message(data_3))

    proxy_barrier = ProxyBarrier(barrier_config, messaging, state=state)  # type: ignore

    threading.Thread(target=proxy_barrier.start).start()

    expected_fwd_1 = {
        "transaction_id": "test_filter_proxy_barrier#1",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_fwd_2 = {
        "transaction_id": "test_filter_proxy_barrier#2",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_fwd_3 = {
        "transaction_id": "test_filter_proxy_barrier#3",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    fwd_1 = messaging.get_msgs_from_queue(filter_1_queue)
    fwd_2 = messaging.get_msgs_from_queue(filter_2_queue)
    fwd_3 = messaging.get_msgs_from_queue(filter_1_queue)

    assert expected_fwd_1 == json.loads(fwd_1)
    assert expected_fwd_2 == json.loads(fwd_2)
    assert expected_fwd_3 == json.loads(fwd_3)

    time.sleep(0.1)
    
    assert len(messaging.exported_msgs[filter_1_queue]) == 0
    assert len(messaging.exported_msgs[filter_2_queue]) == 0
    assert len(messaging.exported_msgs[output_queue]) == 0

    time.sleep(0.1)
    
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)

def test_proxy_barrier_forwards_eof_once_all_were_received_from_same_connection():
    barrier_config = {"FILTER_COUNT": 2, "FILTER_TYPE": "test_filter"}

    conn_id_1 = 5
    conn_id_2 = 8
    controller_id = f"{barrier_config.get('FILTER_TYPE')}_proxy_barrier"
    file_path = f"test/{controller_id}.json"
    temp_file_path = f"test/{controller_id}.tmp"

    extra_fields = {
        "type_last_message": ProxyBarrier.DATA,
        "proxy.EOF": False,
        "queries": [],
        "conn_id": 0,
        "proxy.current_queue": 0,
        "proxy.data": [],
        "barrier.eof_count": {},
        "barrier.forward_to": "",
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    if os.path.exists(state.file_path):
        state.update_from_file()

    input_queue = "test_filter_queue"
    eof_queue = "test_filter_eof"
    filter_1_queue = "test_filter1"
    filter_2_queue = "test_filter2"
    output_queue = "output_queue"

    messaging = MockMessaging(
        host="test",
        port=1234,
        queues_to_export=[filter_1_queue, filter_2_queue, output_queue],
        msgs_to_consume=4,
    )

    data_1 = {
        "transaction_id": "test#1",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    eof_f1_conn1 = {
        "transaction_id": "test_filter_1#2_EOF",
        "EOF": True,
        "conn_id": conn_id_1,
        "queries": [1],
        "forward_to": [output_queue]
    }
    
    eof_f2_conn2 = {
        "transaction_id": "test_filter_2#2_EOF",
        "EOF": True,
        "conn_id": conn_id_2,
        "queries": [1],
        "forward_to": [output_queue]
    }

    eof_f2_conn1 = {
        "transaction_id": "test_filter_2#3_EOF",
        "EOF": True,
        "conn_id": conn_id_1,
        "queries": [1],
        "forward_to": [output_queue]
    }

    messaging.send_to_queue(input_queue, Message(data_1))
    messaging.send_to_queue(eof_queue, Message(eof_f1_conn1))
    messaging.send_to_queue(eof_queue, Message(eof_f2_conn2))
    messaging.send_to_queue(eof_queue, Message(eof_f2_conn1))

    proxy_barrier = ProxyBarrier(barrier_config, messaging, state=state)  # type: ignore

    threading.Thread(target=proxy_barrier.start).start()

    expected_fwd_1 = {
        "transaction_id": "test_filter_proxy_barrier#1",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_eof = {
        "transaction_id": "test_filter_proxy_barrier#2",
        "conn_id": conn_id_1,
        "queries": [1],
        "EOF": True,
    }

    fwd_1 = messaging.get_msgs_from_queue(filter_1_queue)
    eof = messaging.get_msgs_from_queue(output_queue)

    assert expected_fwd_1 == json.loads(fwd_1)
    assert expected_eof == json.loads(eof)

    time.sleep(0.1)
    
    assert len(messaging.exported_msgs[filter_1_queue]) == 0
    assert len(messaging.exported_msgs[filter_2_queue]) == 0
    assert len(messaging.exported_msgs[output_queue]) == 0

    time.sleep(0.1)
    
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)

def test_proxy_barrier_dispatches_eof_to_all_filters():
    barrier_config = {"FILTER_COUNT": 2, "FILTER_TYPE": "test_filter"}

    conn_id = 5
    controller_id = f"{barrier_config.get('FILTER_TYPE')}_proxy_barrier"
    file_path = f"test/{controller_id}.json"
    temp_file_path = f"test/{controller_id}.tmp"

    extra_fields = {
        "type_last_message": ProxyBarrier.DATA,
        "proxy.EOF": False,
        "queries": [],
        "conn_id": 0,
        "proxy.current_queue": 0,
        "proxy.data": [],
        "barrier.eof_count": {},
        "barrier.forward_to": "",
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    # if os.path.exists(state.file_path):
    #     state.update_from_file()

    input_queue = "test_filter_queue"
    eof_queue = "test_filter_eof"
    filter_1_queue = "test_filter1"
    filter_2_queue = "test_filter2"
    output_queue = "output_queue"

    messaging = MockMessaging(
        host="test",
        port=1234,
        queues_to_export=[filter_1_queue, filter_2_queue, output_queue],
        msgs_to_consume=2,
    )

    data_1 = {
        "transaction_id": "test#1",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    eof_1 = {
        "transaction_id": "test#1_EOF",
        "EOF": True,
        "conn_id": conn_id,
        "queries": [1],
    }


    messaging.send_to_queue(input_queue, Message(data_1))
    messaging.send_to_queue(input_queue, Message(eof_1))

    proxy_barrier = ProxyBarrier(barrier_config, messaging, state=state)  # type: ignore

    threading.Thread(target=proxy_barrier.start).start()

    expected_fwd_1 = {
        "transaction_id": "test_filter_proxy_barrier#1",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_eof = {
        "transaction_id": "test_filter_proxy_barrier#2_EOF",
        "conn_id": conn_id,
        "queries": [1],
        "EOF": True,
    }

    fwd_data_1 = messaging.get_msgs_from_queue(filter_1_queue)
    fwd_eof1 = messaging.get_msgs_from_queue(filter_1_queue)
    fwd_eof2 = messaging.get_msgs_from_queue(filter_2_queue)

    assert expected_fwd_1 == json.loads(fwd_data_1)
    assert expected_eof == json.loads(fwd_eof1)
    assert True
    assert fwd_eof2 == fwd_eof1

    time.sleep(0.1)
    
    assert len(messaging.exported_msgs[filter_1_queue]) == 0
    assert len(messaging.exported_msgs[filter_2_queue]) == 0
    assert len(messaging.exported_msgs[output_queue]) == 0

    time.sleep(0.1)
    
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)

def test_proxy_barrier_forwards_data_in_rr_multiple_connections():
    barrier_config = {"FILTER_COUNT": 2, "FILTER_TYPE": "test_filter"}

    conn_id_1 = 5
    conn_id_2 = 8
    controller_id = f"{barrier_config.get('FILTER_TYPE')}_proxy_barrier"
    file_path = f"test/{controller_id}.json"
    temp_file_path = f"test/{controller_id}.tmp"

    extra_fields = {
        "type_last_message": ProxyBarrier.DATA,
        "proxy.EOF": False,
        "queries": [],
        "conn_id": 0,
        "proxy.current_queue": 0,
        "proxy.data": [],
        "barrier.eof_count": {},
        "barrier.forward_to": "",
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    if os.path.exists(state.file_path):
        state.update_from_file()

    input_queue = "test_filter_queue"
    filter_1_queue = "test_filter1"
    filter_2_queue = "test_filter2"
    output_queue = "output_queue"

    messaging = MockMessaging(
        host="test",
        port=1234,
        queues_to_export=[filter_1_queue, filter_2_queue, output_queue],
        msgs_to_consume=3,
    )

    data_1 = {
        "transaction_id": "test#1",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    data_2 = {
        "transaction_id": "test#2",
        "conn_id": conn_id_2,
        "queries": [1],
        "data": [
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    data_3 = {
        "transaction_id": "test#3",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    messaging.send_to_queue(input_queue, Message(data_1))
    messaging.send_to_queue(input_queue, Message(data_2))
    messaging.send_to_queue(input_queue, Message(data_3))

    proxy_barrier = ProxyBarrier(barrier_config, messaging, state=state)  # type: ignore

    threading.Thread(target=proxy_barrier.start).start()

    expected_fwd_1 = {
        "transaction_id": "test_filter_proxy_barrier#1",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_fwd_2 = {
        "transaction_id": "test_filter_proxy_barrier#2",
        "conn_id": conn_id_2,
        "queries": [1],
        "data": [
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_fwd_3 = {
        "transaction_id": "test_filter_proxy_barrier#3",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    fwd_1 = messaging.get_msgs_from_queue(filter_1_queue)
    fwd_2 = messaging.get_msgs_from_queue(filter_2_queue)
    fwd_3 = messaging.get_msgs_from_queue(filter_1_queue)

    assert expected_fwd_1 == json.loads(fwd_1)
    assert expected_fwd_2 == json.loads(fwd_2)
    assert expected_fwd_3 == json.loads(fwd_3)

    time.sleep(0.1)
    
    assert len(messaging.exported_msgs[filter_1_queue]) == 0
    assert len(messaging.exported_msgs[filter_2_queue]) == 0
    assert len(messaging.exported_msgs[output_queue]) == 0

    time.sleep(0.1)
    
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)

def test_proxy_barrier_recovers_from_crash_distributing_data():
    barrier_config = {"FILTER_COUNT": 2, "FILTER_TYPE": "test_filter"}

    conn_id = 5
    controller_id = f"{barrier_config.get('FILTER_TYPE')}_proxy_barrier"
    file_path = f"test/{controller_id}.json"
    temp_file_path = f"test/{controller_id}.tmp"

    extra_fields = {
        "type_last_message": ProxyBarrier.DATA,
        "proxy.EOF": False,
        "queries": [],
        "conn_id": 0,
        "proxy.current_queue": 0,
        "proxy.data": [],
        "barrier.eof_count": {},
        "barrier.forward_to": "",
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    # if os.path.exists(state.file_path):
    #     state.update_from_file()

    input_queue = "test_filter_queue"
    filter_1_queue = "test_filter1"
    filter_2_queue = "test_filter2"
    output_queue = "output_queue"

    messaging = MockMessaging(
        host="test",
        port=1234,
        queues_to_export=[filter_1_queue, filter_2_queue, output_queue],
        msgs_to_consume=3,
        crash_on_send=5
    )

    data_1 = {
        "transaction_id": "test#1",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    data_2 = {
        "transaction_id": "test#2",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    data_3 = {
        "transaction_id": "test#3",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    messaging.send_to_queue(input_queue, Message(data_1))
    messaging.send_to_queue(input_queue, Message(data_2))
    messaging.send_to_queue(input_queue, Message(data_3))

    proxy_barrier = ProxyBarrier(barrier_config, messaging, state=state)  # type: ignore

    try:
        threading.Thread(target=proxy_barrier.start).start()
    except ProvokedError:
        pass


    time.sleep(0.1)

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    if os.path.exists(state.file_path):
        state.update_from_file()

    proxy_barrier = ProxyBarrier(barrier_config, messaging, state=state)  # type: ignore
    
    threading.Thread(target=proxy_barrier.start).start()

    expected_fwd_1 = {
        "transaction_id": "test_filter_proxy_barrier#1",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_fwd_2 = {
        "transaction_id": "test_filter_proxy_barrier#2",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_fwd_3 = {
        "transaction_id": "test_filter_proxy_barrier#3",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    fwd_1 = messaging.get_msgs_from_queue(filter_1_queue)
    fwd_2 = messaging.get_msgs_from_queue(filter_2_queue)
    fwd_3 = messaging.get_msgs_from_queue(filter_1_queue)

    assert expected_fwd_1 == json.loads(fwd_1)
    assert expected_fwd_2 == json.loads(fwd_2)
    assert expected_fwd_3 == json.loads(fwd_3)

    time.sleep(0.1)
    
    assert len(messaging.exported_msgs[filter_1_queue]) == 0
    assert len(messaging.exported_msgs[filter_2_queue]) == 0
    assert len(messaging.exported_msgs[output_queue]) == 0

    time.sleep(0.1)
    
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)

def test_proxy_barrier_recovers_from_crash_forwarding_eof_to_output_queue():
    barrier_config = {"FILTER_COUNT": 2, "FILTER_TYPE": "test_filter"}

    conn_id_1 = 5
    conn_id_2 = 8
    controller_id = f"{barrier_config.get('FILTER_TYPE')}_proxy_barrier"
    file_path = f"test/{controller_id}.json"
    temp_file_path = f"test/{controller_id}.tmp"

    extra_fields = {
        "type_last_message": ProxyBarrier.DATA,
        "proxy.EOF": False,
        "queries": [],
        "conn_id": 0,
        "proxy.current_queue": 0,
        "proxy.data": [],
        "barrier.eof_count": {},
        "barrier.forward_to": "",
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    if os.path.exists(state.file_path):
        state.update_from_file()

    input_queue = "test_filter_queue"
    eof_queue = "test_filter_eof"
    filter_1_queue = "test_filter1"
    filter_2_queue = "test_filter2"
    output_queue = "output_queue"

    messaging = MockMessaging(
        host="test",
        port=1234,
        queues_to_export=[filter_1_queue, filter_2_queue, output_queue],
        msgs_to_consume=4,
        crash_on_send=6
    )

    data_1 = {
        "transaction_id": "test#1",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    eof_f1_conn1 = {
        "transaction_id": "test_filter_1#2_EOF",
        "EOF": True,
        "conn_id": conn_id_1,
        "queries": [1],
        "forward_to": [output_queue]
    }
    
    eof_f2_conn2 = {
        "transaction_id": "test_filter_2#2_EOF",
        "EOF": True,
        "conn_id": conn_id_2,
        "queries": [1],
        "forward_to": [output_queue]
    }

    eof_f2_conn1 = {
        "transaction_id": "test_filter_2#3_EOF",
        "EOF": True,
        "conn_id": conn_id_1,
        "queries": [1],
        "forward_to": [output_queue]
    }

    messaging.send_to_queue(input_queue, Message(data_1))
    messaging.send_to_queue(eof_queue, Message(eof_f1_conn1))
    messaging.send_to_queue(eof_queue, Message(eof_f2_conn2))
    messaging.send_to_queue(eof_queue, Message(eof_f2_conn1))

    proxy_barrier = ProxyBarrier(barrier_config, messaging, state=state)  # type: ignore

    try:
        threading.Thread(target=proxy_barrier.start).start()
    except ProvokedError:
        pass

    time.sleep(0.1)

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    if os.path.exists(state.file_path):
        state.update_from_file()
    
    proxy_barrier = ProxyBarrier(barrier_config, messaging, state=state)  # type: ignore
    threading.Thread(target=proxy_barrier.start).start()


    expected_fwd_1 = {
        "transaction_id": "test_filter_proxy_barrier#1",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_eof = {
        "transaction_id": "test_filter_proxy_barrier#2",
        "conn_id": conn_id_1,
        "queries": [1],
        "EOF": True,
    }

    fwd_1 = messaging.get_msgs_from_queue(filter_1_queue)
    eof = messaging.get_msgs_from_queue(output_queue)

    assert expected_fwd_1 == json.loads(fwd_1)
    assert expected_eof == json.loads(eof)

    time.sleep(0.1)
    
    assert len(messaging.exported_msgs[filter_1_queue]) == 0
    assert len(messaging.exported_msgs[filter_2_queue]) == 0
    assert len(messaging.exported_msgs[output_queue]) == 0

    time.sleep(0.1)
    
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)

def test_proxy_barrier_recovers_from_crash_dispatching_eof_to_its_filters():
    barrier_config = {"FILTER_COUNT": 2, "FILTER_TYPE": "test_filter"}

    conn_id = 5
    controller_id = f"{barrier_config.get('FILTER_TYPE')}_proxy_barrier"
    file_path = f"test/{controller_id}.json"
    temp_file_path = f"test/{controller_id}.tmp"

    extra_fields = {
        "type_last_message": ProxyBarrier.DATA,
        "proxy.EOF": False,
        "queries": [],
        "conn_id": 0,
        "proxy.current_queue": 0,
        "proxy.data": [],
        "barrier.eof_count": {},
        "barrier.forward_to": "",
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    # if os.path.exists(state.file_path):
    #     state.update_from_file()

    input_queue = "test_filter_queue"
    eof_queue = "test_filter_eof"
    filter_1_queue = "test_filter1"
    filter_2_queue = "test_filter2"
    output_queue = "output_queue"

    messaging = MockMessaging(
        host="test",
        port=1234,
        queues_to_export=[filter_1_queue, filter_2_queue, output_queue],
        msgs_to_consume=1,
        crash_on_send=3
    )

    eof_1 = {
        "transaction_id": "test#1_EOF",
        "EOF": True,
        "conn_id": conn_id,
        "queries": [1],
    }

    messaging.send_to_queue(input_queue, Message(eof_1))

    proxy_barrier = ProxyBarrier(barrier_config, messaging, state=state)  # type: ignore

    try:
        threading.Thread(target=proxy_barrier.start).start()
    except ProvokedError:
        pass

    time.sleep(0.1)

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    if os.path.exists(state.file_path):
        state.update_from_file()
    
    proxy_barrier = ProxyBarrier(barrier_config, messaging, state=state)  # type: ignore
    threading.Thread(target=proxy_barrier.start).start()



    expected_eof = {
        "transaction_id": "test_filter_proxy_barrier#1_EOF",
        "conn_id": conn_id,
        "queries": [1],
        "EOF": True,
    }

    fwd_eof1 = messaging.get_msgs_from_queue(filter_1_queue)
    fwd_eof1_dup = messaging.get_msgs_from_queue(filter_1_queue)
    fwd_eof2 = messaging.get_msgs_from_queue(filter_2_queue)

    assert expected_eof == json.loads(fwd_eof1)
    assert fwd_eof2 == fwd_eof1
    assert fwd_eof1_dup == fwd_eof1

    time.sleep(0.1)
    
    assert len(messaging.exported_msgs[filter_1_queue]) == 0
    assert len(messaging.exported_msgs[filter_2_queue]) == 0
    assert len(messaging.exported_msgs[output_queue]) == 0


    time.sleep(0.1)
    
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
