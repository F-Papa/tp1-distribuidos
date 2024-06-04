# 1 Levantar un filtro
# 2 Crashearlo en el medio de un mensaje
# 3 Volver a levantarlo
# Verificar los resultados


import json
import os
import threading
import time
from typing import Callable
from src.controller_state.controller_state import ControllerState
from src.controllers.filters.title_filter.main import TitleFilter
from src.exceptions.shutting_down import ShuttingDown
from src.messaging.message import Message
from test.mocks.mock_messaging import MockMessaging, ProvokedError


def test_title_filter_works_if_no_faults():
    controller_id = "title_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"

    # Set up
    extra_fields = {
        "filtered_books": [],
        "conn_id": 0,
        "queries": [],
        "EOF": False,
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    # Mock Messaging Server to work as IPC
    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=["output_title_filter_test", "eof_title_filter_test"],
        msgs_to_consume=1,
    )

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": 99,
    }

    # Mock message to be sent to the filter
    msg_body = {
        "transaction_id": "test#1",
        "queries": [1],
        "conn_id": 99,
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "superman2", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
        "EOF": True,
    }

    messaging.send_to_queue("title_filter99", Message(msg_body))

    # Start the filter
    filter = TitleFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        title_keyword="test",
        eof_queue="eof_title_filter_test",
        output_queue="output_title_filter_test",
    )
    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data = {
        "transaction_id": "title_filter_test#1",
        "conn_id": 99,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_eof = {
        "transaction_id": "title_filter_test#1_EOF",
        "conn_id": 99,
        "EOF": True,
        "forward_to": ["output_title_filter_test"],
        "queries": [1],
    }

    # Get actual results
    data = messaging.get_msgs_from_queue("output_title_filter_test")
    eof = messaging.get_msgs_from_queue("eof_title_filter_test")

    assert json.loads(eof) == expected_eof
    assert json.loads(data) == expected_data

    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)


def test_title_filter_ignores_duplicate_transactions():
    controller_id = "title_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"

    # Set up
    extra_fields = {
        "filtered_books": [],
        "conn_id": 0,
        "queries": [],
        "EOF": False,
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    # Mock Messaging Server to work as IPC
    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=["output_title_filter_test", "eof_title_filter_test"],
        msgs_to_consume=3,
    )

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": 99,
    }

    # Mock message to be sent to the filter
    msg_body1 = {
        "transaction_id": "test#1",
        "queries": [1],
        "conn_id": 99,
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "superman2", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    msg_body2 = {
        "transaction_id": "test#2",
        "queries": [1],
        "conn_id": 99,
        "data": [
            {"title": "test4", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "hello", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test5", "categories": ["test_cat"], "publisher": "test_pub"},
            {
                "title": "cinderella",
                "categories": ["test_cat"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    messaging.send_to_queue("title_filter99", Message(msg_body1))
    messaging.send_to_queue("title_filter99", Message(msg_body1))
    messaging.send_to_queue("title_filter99", Message(msg_body2))

    # Start the filter
    filter = TitleFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        title_keyword="test",
        eof_queue="eof_title_filter_test",
        output_queue="output_title_filter_test",
    )
    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data1 = {
        "transaction_id": "title_filter_test#1",
        "conn_id": 99,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_data2 = {
        "transaction_id": "title_filter_test#2",
        "conn_id": 99,
        "queries": [1],
        "data": [
            {"title": "test4", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test5", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_eof = {
        "transaction_id": "title_filter_test#2_EOF",
        "conn_id": 99,
        "EOF": True,
        "forward_to": ["output_title_filter_test"],
        "queries": [1],
    }

    # Get actual results
    data_1 = messaging.get_msgs_from_queue("output_title_filter_test")
    data_2 = messaging.get_msgs_from_queue("output_title_filter_test")
    eof = messaging.get_msgs_from_queue("eof_title_filter_test")

    assert json.loads(eof) == expected_eof
    assert json.loads(data_1) == expected_data1
    assert json.loads(data_2) == expected_data2

    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)


def test_title_filter_works_if_no_faults_multiple_messages():
    controller_id = "title_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"

    # Set up
    extra_fields = {
        "filtered_books": [],
        "conn_id": 0,
        "queries": [],
        "EOF": False,
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    # Mock Messaging Server to work as IPC
    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=["output_title_filter_test", "eof_title_filter_test"],
        msgs_to_consume=2,
    )

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": 99,
    }

    # Mock message to be sent to the filter
    msg_body1 = {
        "transaction_id": "test#1",
        "queries": [1],
        "conn_id": 99,
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "superman2", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    msg_body2 = {
        "transaction_id": "test#2",
        "queries": [1],
        "conn_id": 99,
        "data": [
            {"title": "test4", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "hello", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test5", "categories": ["test_cat"], "publisher": "test_pub"},
            {
                "title": "cinderella",
                "categories": ["test_cat"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    messaging.send_to_queue("title_filter99", Message(msg_body1))
    messaging.send_to_queue("title_filter99", Message(msg_body2))

    # Start the filter
    filter = TitleFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        title_keyword="test",
        eof_queue="eof_title_filter_test",
        output_queue="output_title_filter_test",
    )
    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data1 = {
        "transaction_id": "title_filter_test#1",
        "conn_id": 99,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_data2 = {
        "transaction_id": "title_filter_test#2",
        "conn_id": 99,
        "queries": [1],
        "data": [
            {"title": "test4", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test5", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_eof = {
        "transaction_id": "title_filter_test#2_EOF",
        "conn_id": 99,
        "EOF": True,
        "forward_to": ["output_title_filter_test"],
        "queries": [1],
    }

    # Get actual results
    data1 = messaging.get_msgs_from_queue("output_title_filter_test")
    data2 = messaging.get_msgs_from_queue("output_title_filter_test")
    eof = messaging.get_msgs_from_queue("eof_title_filter_test")

    assert json.loads(eof) == expected_eof
    assert json.loads(data1) == expected_data1
    assert json.loads(data2) == expected_data2

    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)


def test_title_filter_works_if_no_faults_multiple_messages_and_connections():
    controller_id = "title_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"

    # Set up
    extra_fields = {
        "filtered_books": [],
        "conn_id": 0,
        "queries": [],
        "EOF": False,
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    # Mock Messaging Server to work as IPC
    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=["output_title_filter_test", "eof_title_filter_test"],
        msgs_to_consume=2,
    )

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": 99,
    }

    # Mock message to be sent to the filter
    msg_body1 = {
        "transaction_id": "test#1",
        "queries": [1],
        "conn_id": 99,
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "superman2", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    msg_body2 = {
        "transaction_id": "test#2",
        "queries": [1],
        "conn_id": 100,
        "data": [
            {"title": "test4", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "hello", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test5", "categories": ["test_cat"], "publisher": "test_pub"},
            {
                "title": "cinderella",
                "categories": ["test_cat"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    messaging.send_to_queue("title_filter99", Message(msg_body1))
    messaging.send_to_queue("title_filter99", Message(msg_body2))

    # Start the filter
    filter = TitleFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        title_keyword="test",
        eof_queue="eof_title_filter_test",
        output_queue="output_title_filter_test",
    )
    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data1 = {
        "transaction_id": "title_filter_test#1",
        "conn_id": 99,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_data2 = {
        "transaction_id": "title_filter_test#2",
        "conn_id": 100,
        "queries": [1],
        "data": [
            {"title": "test4", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test5", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_eof = {
        "transaction_id": "title_filter_test#2_EOF",
        "conn_id": 100,
        "EOF": True,
        "forward_to": ["output_title_filter_test"],
        "queries": [1],
    }

    # Get actual results
    data1 = messaging.get_msgs_from_queue("output_title_filter_test")
    data2 = messaging.get_msgs_from_queue("output_title_filter_test")
    eof = messaging.get_msgs_from_queue("eof_title_filter_test")

    assert json.loads(eof) == expected_eof
    assert json.loads(data1) == expected_data1
    assert json.loads(data2) == expected_data2

    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)


def test_title_filter_recovers_from_crash_sending_data():

    controller_id = "title_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"

    # Set up
    extra_fields = {
        "filtered_books": [],
        "conn_id": 0,
        "queries": [],
        "EOF": False,
    }

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    # Mock Messaging Server to work as IPC
    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=["output_title_filter_test", "eof_title_filter_test"],
        msgs_to_consume=2,
        crash_on_send=3,  # The first will be sent from the test, the second from the filter
    )

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": 99,
    }

    # Mock message to be sent to the filter
    msg_body1 = {
        "transaction_id": "test#1",
        "queries": [1],
        "conn_id": 99,
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "superman2", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    msg_body2 = {
        "transaction_id": "test#2",
        "queries": [1],
        "conn_id": 99,
        "data": [
            {"title": "test4", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "hello", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test5", "categories": ["test_cat"], "publisher": "test_pub"},
            {
                "title": "cinderella",
                "categories": ["test_cat"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    messaging.send_to_queue("title_filter99", Message(msg_body1))
    messaging.send_to_queue("title_filter99", Message(msg_body2))

    # Start the filter
    filter1 = TitleFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        title_keyword="test",
        eof_queue="eof_title_filter_test",
        output_queue="output_title_filter_test",
    )

    # Start it once and make it crash
    thread_1 = threading.Thread(target=filter1.start)
    try:
        thread_1.start()
    except ProvokedError:
        pass

    # Start it again and make it recover

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )
    time.sleep(0.1)

    filter2 = TitleFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        title_keyword="test",
        eof_queue="eof_title_filter_test",
        output_queue="output_title_filter_test",
    )
    threading.Thread(target=filter2.start).start()

    # Define expected results
    expected_data1 = {
        "transaction_id": "title_filter_test#1",
        "conn_id": 99,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_data2 = {
        "transaction_id": "title_filter_test#2",
        "conn_id": 99,
        "queries": [1],
        "data": [
            {"title": "test4", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test5", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_eof = {
        "transaction_id": "title_filter_test#2_EOF",
        "conn_id": 99,
        "EOF": True,
        "forward_to": ["output_title_filter_test"],
        "queries": [1],
    }

    # Get actual results
    data1 = messaging.get_msgs_from_queue("output_title_filter_test")
    data2 = messaging.get_msgs_from_queue("output_title_filter_test")
    eof = messaging.get_msgs_from_queue("eof_title_filter_test")

    assert json.loads(eof) == expected_eof
    assert json.loads(data1) == expected_data1
    assert json.loads(data2) == expected_data2

    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
