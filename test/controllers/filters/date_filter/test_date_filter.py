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
from src.controllers.filters.category_filter.main import CategoryFilter
from src.controllers.filters.date_filter.main import DateFilter
from src.controllers.filters.title_filter.main import TitleFilter
from src.exceptions.shutting_down import ShuttingDown
from src.messaging.message import Message
from test.mocks.mock_messaging import MockMessaging, ProvokedError


def test_category_filter_works_if_no_faults():
    controller_id = "date_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"
    filter_number = 67
    conn_id = 99

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body = {
        "transaction_id": 1,
        "queries": [1, 3, 4],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test1",
                "year": 2004,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
            {
                "title": "test2",
                "year": 1991,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
            {
                "title": "test3",
                "year": 2020,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
            {
                "title": "test4",
                "year": 2000,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    # Mock Messaging Server to work as IPC
    output_queue_q1 = "output1_date_filter_test"
    output_queue_q3_4_prefix = "output3_4_date_filter_test"

    messaging = MockMessaging(
        sender_id=controller_id,
        host="localhost",
        port=5672,
        times_to_listen=1,
    )

    # Start the filter
    state = DateFilter.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    filter = DateFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        output_q3_4_prefix=output_queue_q3_4_prefix,
        output_q1=output_queue_q1,
        upper_q1=2020,
        lower_q1=2000,
        upper_q3_4=1999,
        lower_q3_4=1990,
    )

    messaging.send_to_queue(
        filter.input_queue(), Message(msg_body), sender_id="another_filter"
    )

    threading.Thread(target=filter.start).start()

    # Define expected resultsj
    expected_q1 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [1],
        "EOF": True,
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test4", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_q3_4 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "EOF": True,
        "queries": [3, 4],
        "data": [
            {"title": "test2", "authors": ["peter"]},
        ],
    }

    # Get actual results
    data_q1 = messaging.get_msgs_from_queue(filter.output_queue_q1())
    data_q3_4 = messaging.get_msgs_from_queue(filter.output_queue_q3_4(conn_id))

    # assert json.dumps(expected_eof_q1) == eof_q1
    assert json.loads(data_q1) == expected_q1
    assert json.loads(data_q3_4) == expected_q3_4

    for q in messaging.queued_msgs:
        assert len(messaging.queued_msgs[q]) == 0
    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)


def test_category_filter_works_if_no_faults_multiple_messages():
    controller_id = "date_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"
    filter_number = 67
    conn_id = 99

    # Set up

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body_1 = {
        "transaction_id": 1,
        "queries": [1, 3, 4],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test1",
                "year": 2004,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
            {
                "title": "test2",
                "year": 1991,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
            {
                "title": "test3",
                "year": 2020,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
            {
                "title": "test4",
                "year": 2000,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
        ],
    }

    msg_body_2 = {
        "transaction_id": 2,
        "queries": [1, 3, 4],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test6",
                "year": 2006,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    # Mock Messaging Server to work as IPC
    output_queue_q1 = "output1_date_filter_test"
    output_queue_q3_4_prefix = "output3_4_date_filter_test"

    messaging = MockMessaging(
        sender_id=controller_id,
        host="localhost",
        port=5672,
        times_to_listen=3,
    )

    state = DateFilter.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    filter = DateFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        output_q3_4_prefix=output_queue_q3_4_prefix,
        output_q1=output_queue_q1,
        upper_q1=2020,
        lower_q1=2000,
        upper_q3_4=1999,
        lower_q3_4=1990,
    )
    messaging.send_to_queue(
        filter.input_queue(), Message(msg_body_1), sender_id="another_filter"
    )
    messaging.send_to_queue(
        filter.input_queue(), Message(msg_body_2), sender_id="another_filter"
    )

    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_1_q1 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test4", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_1_q3_4 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [3, 4],
        "data": [
            {"title": "test2", "authors": ["peter"]},
        ],
    }

    expected_2_q1 = {
        "sender": controller_id,
        "transaction_id": 2,
        "conn_id": conn_id,
        "queries": [1],
        "EOF": True,
        "data": [
            {"title": "test6", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_2_q3_4 = {
        "sender": controller_id,
        "transaction_id": 2,
        "conn_id": conn_id,
        "queries": [3, 4],
        "EOF": True,
    }

    # Get actual results
    actual_1_q1 = messaging.get_msgs_from_queue(filter.output_queue_q1())
    actual_1_q3_4 = messaging.get_msgs_from_queue(filter.output_queue_q3_4(conn_id))
    actual_2_q1 = messaging.get_msgs_from_queue(filter.output_queue_q1())
    actual_2_q3_4 = messaging.get_msgs_from_queue(filter.output_queue_q3_4(conn_id))

    assert json.loads(actual_1_q1) == expected_1_q1
    assert json.loads(actual_1_q3_4) == expected_1_q3_4
    assert json.loads(actual_2_q1) == expected_2_q1
    assert json.loads(actual_2_q3_4) == expected_2_q3_4
    for q in messaging.queued_msgs:
        assert len(messaging.queued_msgs[q]) == 0
    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)


def test_category_filter_recovers_from_crash_on_first_send():
    controller_id = "date_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"
    filter_number = 67
    conn_id = 99

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body_1 = {
        "transaction_id": 1,
        "queries": [1, 3, 4],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test1",
                "year": 2004,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
            {
                "title": "test2",
                "year": 1991,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
            {
                "title": "test3",
                "year": 2020,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
            {
                "title": "test4",
                "year": 2000,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
        ],
    }

    msg_body_2 = {
        "transaction_id": 2,
        "queries": [1, 3, 4],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test6",
                "year": 2006,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    # Mock Messaging Server to work as IPC
    input_queue = "date_filter" + str(filter_number)
    output_queue_q1 = "output1_date_filter_test"
    output_queue_q3_4_prefix = "output3_4_date_filter_test"

    messaging = MockMessaging(
        sender_id=controller_id,
        host="localhost",
        port=5672,
        times_to_listen=4,
        crash_on_send=3,
    )

    messaging.send_to_queue(
        input_queue, Message(msg_body_1), sender_id="another_filter"
    )
    messaging.send_to_queue(
        input_queue, Message(msg_body_2), sender_id="another_filter"
    )

    state = DateFilter.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    filter = DateFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        output_q3_4_prefix=output_queue_q3_4_prefix,
        output_q1=output_queue_q1,
        upper_q1=2020,
        lower_q1=2000,
        upper_q3_4=1999,
        lower_q3_4=1990,
    )

    try:
        threading.Thread(target=filter.start).start()
    except ProvokedError:
        pass

    time.sleep(0.1)

    state = DateFilter.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    if os.path.exists(file_path):
        state.update_from_file()

    filter = DateFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        output_q3_4_prefix=output_queue_q3_4_prefix,
        output_q1=output_queue_q1,
        upper_q1=2020,
        lower_q1=2000,
        upper_q3_4=1999,
        lower_q3_4=1990,
    )
    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_1_q1 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test4", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_1_q3_4 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [3, 4],
        "data": [
            {"title": "test2", "authors": ["peter"]},
        ],
    }

    expected_2_q1 = {
        "sender": controller_id,
        "transaction_id": 2,
        "conn_id": conn_id,
        "EOF": True,
        "queries": [1],
        "data": [
            {"title": "test6", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_2_q3_4 = {
        "sender": controller_id,
        "transaction_id": 2,
        "conn_id": conn_id,
        "queries": [3, 4],
        "EOF": True,
    }

    # Get actual results
    actual_1_q1 = messaging.get_msgs_from_queue(filter.output_queue_q1())
    actual_2_q1 = messaging.get_msgs_from_queue(filter.output_queue_q1())
    actual_1_q3_4 = messaging.get_msgs_from_queue(filter.output_queue_q3_4(conn_id))
    actual_2_q3_4 = messaging.get_msgs_from_queue(filter.output_queue_q3_4(conn_id))

    assert json.loads(actual_1_q1) == expected_1_q1
    assert json.loads(actual_2_q1) == expected_2_q1
    assert json.loads(actual_1_q3_4) == expected_1_q3_4
    assert json.loads(actual_2_q3_4) == expected_2_q3_4

    for q in messaging.queued_msgs:
        assert len(messaging.queued_msgs[q]) == 0

    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)


def test_category_filter_recovers_from_crash_on_second_send():
    controller_id = "date_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"
    filter_number = 67
    conn_id = 99

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body_1 = {
        "transaction_id": 1,
        "queries": [1, 3, 4],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test1",
                "year": 2004,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
            {
                "title": "test2",
                "year": 1991,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
            {
                "title": "test3",
                "year": 2020,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
            {
                "title": "test4",
                "year": 2000,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
        ],
    }

    msg_body_2 = {
        "transaction_id": 2,
        "queries": [1, 3, 4],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test6",
                "year": 2006,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    # Mock Messaging Server to work as IPC
    output_queue_q1 = "output1_date_filter_test"
    output_queue_q3_4_prefix = "output3_4_date_filter_test"

    messaging = MockMessaging(
        sender_id=controller_id,
        host="localhost",
        port=5672,
        times_to_listen=4,
        crash_on_send=4,
    )

    state = DateFilter.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    filter = DateFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        output_q3_4_prefix=output_queue_q3_4_prefix,
        output_q1=output_queue_q1,
        upper_q1=2020,
        lower_q1=2000,
        upper_q3_4=1999,
        lower_q3_4=1990,
    )

    messaging.send_to_queue(
        filter.input_queue(), Message(msg_body_1), sender_id="another_filter"
    )
    messaging.send_to_queue(
        filter.input_queue(), Message(msg_body_2), sender_id="another_filter"
    )

    try:
        threading.Thread(target=filter.start).start()
    except ProvokedError:
        pass

    time.sleep(0.1)

    state = DateFilter.default_state(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
    )

    if os.path.exists(file_path):
        state.update_from_file()

    filter = DateFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        output_q3_4_prefix=output_queue_q3_4_prefix,
        output_q1=output_queue_q1,
        upper_q1=2020,
        lower_q1=2000,
        upper_q3_4=1999,
        lower_q3_4=1990,
    )
    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_1_q1 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test4", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_1_q3_4 = {
        "sender": controller_id,
        "transaction_id": 1,
        "conn_id": conn_id,
        "queries": [3, 4],
        "data": [
            {"title": "test2", "authors": ["peter"]},
        ],
    }

    expected_2_q1 = {
        "sender": controller_id,
        "transaction_id": 2,
        "conn_id": conn_id,
        "EOF": True,
        "queries": [1],
        "data": [
            {"title": "test6", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_2_q3_4 = {
        "sender": controller_id,
        "transaction_id": 2,
        "conn_id": conn_id,
        "queries": [3, 4],
        "EOF": True,
    }

    # Get actual results
    actual_1_q1 = messaging.get_msgs_from_queue(filter.output_queue_q1())
    actual_1_q1_dup = messaging.get_msgs_from_queue(filter.output_queue_q1())
    actual_1_q3_4 = messaging.get_msgs_from_queue(filter.output_queue_q3_4(conn_id))
    actual_2_q1 = messaging.get_msgs_from_queue(filter.output_queue_q1())
    actual_2_q3_4 = messaging.get_msgs_from_queue(filter.output_queue_q3_4(conn_id))

    assert json.loads(actual_1_q1) == expected_1_q1
    assert json.loads(actual_1_q1_dup) == expected_1_q1
    assert json.loads(actual_1_q3_4) == expected_1_q3_4
    assert json.loads(actual_2_q1) == expected_2_q1
    assert json.loads(actual_2_q3_4) == expected_2_q3_4

    for q in messaging.queued_msgs:
        assert len(messaging.queued_msgs[q]) == 0

    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
