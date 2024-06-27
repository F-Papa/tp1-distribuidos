# 1 Levantar un filtro
# 2 Crashearlo en el medio de un mensaje
# 3 Volver a levantarlo
# Verificar los resultados


import json
import os
import threading
import time
from typing import Callable

import pytest
from src.controller_state.controller_state import ControllerState
from src.controllers.filters.title_filter.main import TitleFilter
from src.exceptions.shutting_down import ShuttingDown
from src.messaging.message import Message
from test.mocks.mock_messaging import MockMessaging, ProvokedError

def test_title_filter_works_if_no_faults():
    controller_id = "title_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"
    output_queue = "output_title_filter_test"
    filter_number = 4
    conn_id = 99

    state = TitleFilter.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    # Mock Messaging Server to work as IPC
    messaging = MockMessaging(
        sender_id=controller_id,
        host="localhost",
        port=5672,
        times_to_listen=1,
    )

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body = {
        "transaction_id": 1,
        "queries": [1],
        "conn_id": conn_id,
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "superman2", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
        "EOF": True,
    }

    # Start the filter
    filter = TitleFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        title_keyword="test",
        output_queue=output_queue,
    )

    messaging.send_to_queue(filter.input_queue(), Message(msg_body), sender_id="another_filter")

    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data = {
        "sender":  controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_eof = {
        "sender":  controller_id,
        "transaction_id": 2,
        "conn_id": conn_id,
        "EOF": True,
        "forward_to": ["output_title_filter_test"],
        "queries": [1],
    }

    # Get actual results
    data = messaging.get_msgs_from_queue(filter.output_queue())
    eof = messaging.get_msgs_from_queue(filter.output_queue())

    assert json.loads(data) == expected_data
    assert json.loads(eof) == expected_eof

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
    conn_id = 99

    # Set up
    state = TitleFilter.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    # Mock Messaging Server to work as IPC
    messaging = MockMessaging(
        sender_id=controller_id,
        host="localhost",
        port=5672,
        times_to_listen=3,
    )

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": conn_id,
    }

    # Mock message to be sent to the filter
    msg_body1 = {
        "transaction_id": 1,
        "queries": [1],
        "conn_id": conn_id,
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "superman2", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    msg_body2 = {
        "transaction_id": 2,
        "queries": [1],
        "conn_id": conn_id,
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

    # Start the filter
    filter = TitleFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        title_keyword="test",
        output_queue="output_title_filter_test",
    )

    messaging.send_to_queue(filter.input_queue(), Message(msg_body1), sender_id="another_filter")
    messaging.send_to_queue(filter.input_queue(), Message(msg_body1), sender_id="another_filter")
    messaging.send_to_queue(filter.input_queue(), Message(msg_body2), sender_id="another_filter")


    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data1 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_data2 = {
        "sender": controller_id,
        "transaction_id": 2,
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test4", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test5", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_eof = {
        "sender": controller_id,
        "transaction_id": 3,
        "conn_id": conn_id,
        "EOF": True,
        "forward_to": ["output_title_filter_test"],
        "queries": [1],
    }

    # Get actual results
    data_1 = messaging.get_msgs_from_queue(filter.output_queue())
    data_2 = messaging.get_msgs_from_queue(filter.output_queue())
    eof = messaging.get_msgs_from_queue(filter.output_queue())

    assert json.loads(data_1) == expected_data1
    assert json.loads(data_2) == expected_data2
    assert json.loads(eof) == expected_eof

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
    conn_id = 99

    # Set up
    state = TitleFilter.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    # Mock Messaging Server to work as IPC
    messaging = MockMessaging(
        sender_id=controller_id,
        host="localhost",
        port=5672,
        times_to_listen=2,
    )

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": conn_id,
    }

    # Mock message to be sent to the filter
    msg_body1 = {
        "transaction_id": 1,
        "queries": [1],
        "conn_id": conn_id,
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "superman2", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    msg_body2 = {
        "transaction_id": 2,
        "queries": [1],
        "conn_id": conn_id,
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

    filter = TitleFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        title_keyword="test",
        output_queue="output_title_filter_test",
    )

    messaging.send_to_queue(filter.input_queue(), Message(msg_body1))
    messaging.send_to_queue(filter.input_queue(), Message(msg_body2))

    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data1 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_data2 = {
        "sender": controller_id,
        "transaction_id": 2,
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test4", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test5", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_eof = {
        "sender": controller_id,
        "transaction_id": 3,
        "conn_id": conn_id,
        "EOF": True,
        "forward_to": ["output_title_filter_test"],
        "queries": [1],
    }

    # Get actual results
    data1 = messaging.get_msgs_from_queue(filter.output_queue())
    data2 = messaging.get_msgs_from_queue(filter.output_queue())
    eof = messaging.get_msgs_from_queue(filter.output_queue())

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
    conn_id = 32

    # Set up
    state = TitleFilter.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )


    # Mock Messaging Server to work as IPC
    messaging = MockMessaging(
        sender_id=controller_id,
        host="localhost",
        port=5672,
        times_to_listen=2,
    )

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": conn_id,
    }

    # Mock message to be sent to the filter
    msg_body1 = {
        "transaction_id": 1,
        "queries": [1],
        "conn_id": conn_id,
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "superman2", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    msg_body2 = {
        "transaction_id": 2,
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

    filter = TitleFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        title_keyword="test",
        output_queue="output_title_filter_test",
    )

    messaging.send_to_queue(filter.input_queue(), Message(msg_body1))
    messaging.send_to_queue(filter.input_queue(), Message(msg_body2))

    # Start the filter
    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data1 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_data2 = {
        "sender": controller_id,
        "transaction_id": 2,
        "conn_id": 100,
        "queries": [1],
        "data": [
            {"title": "test4", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test5", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_eof = {
        "sender": controller_id,
        "transaction_id": 3,
        "conn_id": 100,
        "EOF": True,
        "forward_to": ["output_title_filter_test"],
        "queries": [1],
    }

    # Get actual results
    data1 = messaging.get_msgs_from_queue(filter.output_queue())
    data2 = messaging.get_msgs_from_queue(filter.output_queue())
    eof = messaging.get_msgs_from_queue(filter.output_queue())

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
    conn_id = 13

    # Set up
    state = TitleFilter.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    # Mock Messaging Server to work as IPC
    messaging = MockMessaging(
        sender_id=controller_id,
        host="localhost",
        port=5672,
        times_to_listen=4,
        crash_on_send=3,  # The first will be sent from the test, the second from the filter
    )

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": conn_id,
    }

    # Mock message to be sent to the filter
    msg_body1 = {
        "transaction_id": 1,
        "queries": [1],
        "conn_id": conn_id,
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "superman2", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    msg_body2 = {
        "transaction_id": 2,
        "queries": [1],
        "conn_id": conn_id,
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
    
    # Start the filter
    filter = TitleFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        title_keyword="test",
        output_queue="output_title_filter_test",
    )

    messaging.send_to_queue(filter.input_queue(), Message(msg_body1))
    messaging.send_to_queue(filter.input_queue(), Message(msg_body2))


    # Start it once and make it crash
    thread_1 = threading.Thread(target=filter.start)
    try:
        thread_1.start()
    except ProvokedError:
        pass

    # Start it again and make it recover

    state = TitleFilter.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    time.sleep(0.1)

    filter = TitleFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        title_keyword="test",
        output_queue="output_title_filter_test",
    )
    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data1 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test2", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_data2 = {
        "sender": controller_id,
        "transaction_id": 2,
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test4", "categories": ["test_cat"], "publisher": "test_pub"},
            {"title": "test5", "categories": ["test_cat"], "publisher": "test_pub"},
        ],
    }

    expected_eof = {
        "sender": controller_id,
        "transaction_id": 3,
        "conn_id": conn_id,
        "EOF": True,
        "forward_to": ["output_title_filter_test"],
        "queries": [1],
    }

    # Get actual results
    data1 = messaging.get_msgs_from_queue(filter.output_queue())
    data2 = messaging.get_msgs_from_queue(filter.output_queue())
    eof = messaging.get_msgs_from_queue(filter.output_queue())

    assert json.loads(eof) == expected_eof
    assert json.loads(data1) == expected_data1
    assert json.loads(data2) == expected_data2

    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
