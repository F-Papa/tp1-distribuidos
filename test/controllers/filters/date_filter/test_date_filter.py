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

    # Set up
    extra_fields = {
        "filtered_books_q1": [],
        "filtered_books_q3_4": [],
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

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body = {
        "transaction_id": "test#1",
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
    input_queue = "date_filter" + str(filter_number)
    output_queue_q1 = "output1_date_filter_test"
    output_queue_q3_4_prefix = "output3_4_date_filter_test"
    eof_queue = "eof_date_filter_test"

    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=[
            output_queue_q1,
            output_queue_q3_4_prefix + str(conn_id),
            eof_queue,
        ],
        msgs_to_consume=1,
    )

    messaging.add_queues(input_queue)
    messaging.send_to_queue(input_queue, Message(msg_body))

    # Start the filter
    filter = DateFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        eof_queue=eof_queue,
        output_q3_4_prefix=output_queue_q3_4_prefix,
        output_q1=output_queue_q1,
        upper_q1=2020,
        lower_q1=2000,
        upper_q3_4=1999,
        lower_q3_4=1990,
        input_queue=input_queue,
    )

    threading.Thread(target=filter.start).start()

    # Define expected resultsj
    expected_data_q1 = {
        "transaction_id": "date_filter_test#1",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test4", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_data_q3_4 = {
        "transaction_id": "date_filter_test#1",
        "conn_id": conn_id,
        "queries": [3, 4],
        "data": [
            {"title": "test2", "authors": ["peter"]},
        ],
    }

    expected_eof = {
        "transaction_id": "date_filter_test#1_EOF",
        "conn_id": conn_id,
        "queries": [1, 3, 4],
        "forward_to": [output_queue_q1, output_queue_q3_4_prefix + str(conn_id)],
        "EOF": True,
    }

    # Get actual results
    data_q1 = messaging.get_msgs_from_queue(output_queue_q1)
    data_q3_4 = messaging.get_msgs_from_queue(output_queue_q3_4_prefix + str(conn_id))

    eof = messaging.get_msgs_from_queue(eof_queue)

    # assert json.dumps(expected_eof_q1) == eof_q1
    assert json.loads(eof) == expected_eof
    assert json.loads(data_q1) == expected_data_q1
    assert json.loads(data_q3_4) == expected_data_q3_4

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
    extra_fields = {
        "filtered_books_q1": [],
        "filtered_books_q3_4": [],
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

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body_1 = {
        "transaction_id": "test#1",
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
        "transaction_id": "test#2",
        "queries": [1, 3, 4],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test5",
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
    eof_queue = "eof_date_filter_test"

    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=[
            output_queue_q1,
            output_queue_q3_4_prefix + str(conn_id),
            eof_queue,
        ],
        msgs_to_consume=2,
    )

    messaging.add_queues(input_queue)
    messaging.send_to_queue(input_queue, Message(msg_body_1))
    messaging.send_to_queue(input_queue, Message(msg_body_2))

    # Start the filter
    filter = DateFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        eof_queue=eof_queue,
        output_q3_4_prefix=output_queue_q3_4_prefix,
        output_q1=output_queue_q1,
        upper_q1=2020,
        lower_q1=2000,
        upper_q3_4=1999,
        lower_q3_4=1990,
        input_queue=input_queue,
    )

    threading.Thread(target=filter.start).start()

    # Define expected resultsj
    expected_data_q1_1 = {
        "transaction_id": "date_filter_test#1",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test4", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_data_q1_2 = {
        "transaction_id": "date_filter_test#2",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test5", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_data_q3_4 = {
        "transaction_id": "date_filter_test#1",
        "conn_id": conn_id,
        "queries": [3, 4],
        "data": [
            {"title": "test2", "authors": ["peter"]},
        ],
    }

    expected_eof = {
        "transaction_id": "date_filter_test#2_EOF",
        "conn_id": conn_id,
        "queries": [1, 3, 4],
        "forward_to": [output_queue_q1, output_queue_q3_4_prefix + str(conn_id)],
        "EOF": True,
    }

    # Get actual results
    data_q1_1 = messaging.get_msgs_from_queue(output_queue_q1)
    data_q1_2 = messaging.get_msgs_from_queue(output_queue_q1)
    data_q3_4 = messaging.get_msgs_from_queue(output_queue_q3_4_prefix + str(conn_id))

    eof = messaging.get_msgs_from_queue(eof_queue)

    # assert json.dumps(expected_eof_q1) == eof_q1
    assert json.loads(eof) == expected_eof
    assert json.loads(data_q1_1) == expected_data_q1_1
    assert json.loads(data_q1_2) == expected_data_q1_2
    assert json.loads(data_q3_4) == expected_data_q3_4

    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)


def test_category_filter_works_if_no_faults_multiple_messages_and_connections():
    controller_id = "date_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"
    filter_number = 67
    conn_id_1 = 99
    conn_id_2 = 109

    # Set up
    extra_fields = {
        "filtered_books_q1": [],
        "filtered_books_q3_4": [],
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

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body_1_conn_1 = {
        "transaction_id": "test#1",
        "queries": [1, 3, 4],
        "conn_id": conn_id_1,
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

    msg_body_conn_2 = {
        "transaction_id": "test#2",
        "queries": [1, 3, 4],
        "conn_id": conn_id_2,
        "data": [
            {
                "title": "test5",
                "year": 2006,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    msg_body_2_conn_1 = {
        "transaction_id": "test#3",
        "queries": [1, 3, 4],
        "conn_id": conn_id_1,
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
    eof_queue = "eof_date_filter_test"

    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=[
            output_queue_q1,
            output_queue_q3_4_prefix + str(conn_id_1),
            output_queue_q3_4_prefix + str(conn_id_2),
            eof_queue,
        ],
        msgs_to_consume=3,
    )

    messaging.add_queues(input_queue)
    messaging.send_to_queue(input_queue, Message(msg_body_1_conn_1))
    messaging.send_to_queue(input_queue, Message(msg_body_conn_2))
    messaging.send_to_queue(input_queue, Message(msg_body_2_conn_1))

    # Start the filter
    filter = DateFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        eof_queue=eof_queue,
        output_q3_4_prefix=output_queue_q3_4_prefix,
        output_q1=output_queue_q1,
        upper_q1=2020,
        lower_q1=2000,
        upper_q3_4=1999,
        lower_q3_4=1990,
        input_queue=input_queue,
    )

    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data_1_q1_conn_1 = {
        "transaction_id": "date_filter_test#1",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test4", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_data_1_q3_4_conn_1 = {
        "transaction_id": "date_filter_test#1",
        "conn_id": conn_id_1,
        "queries": [3, 4],
        "data": [
            {"title": "test2", "authors": ["peter"]},
        ],
    }

    expected_data_q1_conn_2 = {
        "transaction_id": "date_filter_test#2",
        "conn_id": conn_id_2,
        "queries": [1],
        "data": [
            {"title": "test5", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_data_2_q1_conn_1 = {
        "transaction_id": "date_filter_test#3",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test6", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_eof_conn_2 = {
        "transaction_id": "date_filter_test#2_EOF",
        "conn_id": conn_id_2,
        "queries": [1, 3, 4],
        "forward_to": [output_queue_q1, output_queue_q3_4_prefix + str(conn_id_2)],
        "EOF": True,
    }

    expected_eof_conn_1 = {
        "transaction_id": "date_filter_test#3_EOF",
        "conn_id": conn_id_1,
        "queries": [1, 3, 4],
        "forward_to": [output_queue_q1, output_queue_q3_4_prefix + str(conn_id_1)],
        "EOF": True,
    }

    # Get actual results
    data_1_q1_conn_1 = messaging.get_msgs_from_queue(output_queue_q1)
    data_1_q3_4_conn_1 = messaging.get_msgs_from_queue(
        output_queue_q3_4_prefix + str(conn_id_1)
    )

    data_q1_conn_2 = messaging.get_msgs_from_queue(output_queue_q1)

    data_2_q1_conn_1 = messaging.get_msgs_from_queue(output_queue_q1)

    eof_conn_2 = messaging.get_msgs_from_queue(eof_queue)
    eof_conn_1 = messaging.get_msgs_from_queue(eof_queue)

    assert json.loads(eof_conn_2) == expected_eof_conn_2
    assert json.loads(eof_conn_1) == expected_eof_conn_1
    assert json.loads(data_1_q1_conn_1) == expected_data_1_q1_conn_1
    assert json.loads(data_1_q3_4_conn_1) == expected_data_1_q3_4_conn_1
    assert json.loads(data_q1_conn_2) == expected_data_q1_conn_2
    assert json.loads(data_2_q1_conn_1) == expected_data_2_q1_conn_1

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
    conn_id_1 = 99
    conn_id_2 = 109

    # Set up
    extra_fields = {
        "filtered_books_q1": [],
        "filtered_books_q3_4": [],
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

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body_1_conn_1 = {
        "transaction_id": "test#1",
        "queries": [1, 3, 4],
        "conn_id": conn_id_1,
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

    msg_body_conn_2 = {
        "transaction_id": "test#2",
        "queries": [1, 3, 4],
        "conn_id": conn_id_2,
        "data": [
            {
                "title": "test5",
                "year": 2006,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    msg_body_2_conn_1 = {
        "transaction_id": "test#3",
        "queries": [1, 3, 4],
        "conn_id": conn_id_1,
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
    eof_queue = "eof_date_filter_test"

    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=[
            output_queue_q1,
            output_queue_q3_4_prefix + str(conn_id_1),
            output_queue_q3_4_prefix + str(conn_id_2),
            eof_queue,
        ],
        msgs_to_consume=3,
        crash_on_send=4,
    )

    messaging.add_queues(input_queue)
    messaging.send_to_queue(input_queue, Message(msg_body_1_conn_1))
    messaging.send_to_queue(input_queue, Message(msg_body_conn_2))
    messaging.send_to_queue(input_queue, Message(msg_body_2_conn_1))

    # Start the filter
    filter = DateFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        eof_queue=eof_queue,
        output_q3_4_prefix=output_queue_q3_4_prefix,
        output_q1=output_queue_q1,
        upper_q1=2020,
        lower_q1=2000,
        upper_q3_4=1999,
        lower_q3_4=1990,
        input_queue=input_queue,
    )

    try:
        threading.Thread(target=filter.start).start()
    except ProvokedError:
        pass

    time.sleep(0.1)

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    filter = DateFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        eof_queue=eof_queue,
        output_q3_4_prefix=output_queue_q3_4_prefix,
        output_q1=output_queue_q1,
        upper_q1=2020,
        lower_q1=2000,
        upper_q3_4=1999,
        lower_q3_4=1990,
        input_queue=input_queue,
    )

    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data_1_q1_conn_1 = {
        "transaction_id": "date_filter_test#1",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test4", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_data_1_q3_4_conn_1 = {
        "transaction_id": "date_filter_test#1",
        "conn_id": conn_id_1,
        "queries": [3, 4],
        "data": [
            {"title": "test2", "authors": ["peter"]},
        ],
    }

    expected_data_q1_conn_2 = {
        "transaction_id": "date_filter_test#2",
        "conn_id": conn_id_2,
        "queries": [1],
        "data": [
            {"title": "test5", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_data_2_q1_conn_1 = {
        "transaction_id": "date_filter_test#3",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test6", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_eof_conn_2 = {
        "transaction_id": "date_filter_test#2_EOF",
        "conn_id": conn_id_2,
        "queries": [1, 3, 4],
        "forward_to": [output_queue_q1, output_queue_q3_4_prefix + str(conn_id_2)],
        "EOF": True,
    }

    expected_eof_conn_1 = {
        "transaction_id": "date_filter_test#3_EOF",
        "conn_id": conn_id_1,
        "queries": [1, 3, 4],
        "forward_to": [output_queue_q1, output_queue_q3_4_prefix + str(conn_id_1)],
        "EOF": True,
    }

    # Get actual results
    data_1_q1_conn_1 = messaging.get_msgs_from_queue(output_queue_q1)
    # data_1_q1_conn_1_dup = messaging.get_msgs_from_queue(output_queue_q1)

    data_1_q3_4_conn_1 = messaging.get_msgs_from_queue(
        output_queue_q3_4_prefix + str(conn_id_1)
    )

    data_q1_conn_2 = messaging.get_msgs_from_queue(output_queue_q1)

    data_2_q1_conn_1 = messaging.get_msgs_from_queue(output_queue_q1)

    eof_conn_2 = messaging.get_msgs_from_queue(eof_queue)
    eof_conn_1 = messaging.get_msgs_from_queue(eof_queue)

    assert json.loads(eof_conn_2) == expected_eof_conn_2
    assert json.loads(eof_conn_1) == expected_eof_conn_1
    # assert data_1_q1_conn_1_dup == data_1_q1_conn_1
    assert json.loads(data_1_q1_conn_1) == expected_data_1_q1_conn_1
    assert json.loads(data_1_q3_4_conn_1) == expected_data_1_q3_4_conn_1
    assert json.loads(data_q1_conn_2) == expected_data_q1_conn_2
    assert json.loads(data_2_q1_conn_1) == expected_data_2_q1_conn_1

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
    conn_id_1 = 99
    conn_id_2 = 109

    # Set up
    extra_fields = {
        "filtered_books_q1": [],
        "filtered_books_q3_4": [],
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

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body_1_conn_1 = {
        "transaction_id": "test#1",
        "queries": [1, 3, 4],
        "conn_id": conn_id_1,
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

    msg_body_conn_2 = {
        "transaction_id": "test#2",
        "queries": [1, 3, 4],
        "conn_id": conn_id_2,
        "data": [
            {
                "title": "test5",
                "year": 2006,
                "categories": ["fiction"],
                "authors": ["peter"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    msg_body_2_conn_1 = {
        "transaction_id": "test#3",
        "queries": [1, 3, 4],
        "conn_id": conn_id_1,
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
    eof_queue = "eof_date_filter_test"

    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=[
            output_queue_q1,
            output_queue_q3_4_prefix + str(conn_id_1),
            output_queue_q3_4_prefix + str(conn_id_2),
            eof_queue,
        ],
        msgs_to_consume=3,
        crash_on_send=5,
    )

    messaging.add_queues(input_queue)
    messaging.send_to_queue(input_queue, Message(msg_body_1_conn_1))
    messaging.send_to_queue(input_queue, Message(msg_body_conn_2))
    messaging.send_to_queue(input_queue, Message(msg_body_2_conn_1))

    # Start the filter
    filter = DateFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        eof_queue=eof_queue,
        output_q3_4_prefix=output_queue_q3_4_prefix,
        output_q1=output_queue_q1,
        upper_q1=2020,
        lower_q1=2000,
        upper_q3_4=1999,
        lower_q3_4=1990,
        input_queue=input_queue,
    )

    try:
        threading.Thread(target=filter.start).start()
    except ProvokedError:
        pass

    time.sleep(0.1)

    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    filter = DateFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        eof_queue=eof_queue,
        output_q3_4_prefix=output_queue_q3_4_prefix,
        output_q1=output_queue_q1,
        upper_q1=2020,
        lower_q1=2000,
        upper_q3_4=1999,
        lower_q3_4=1990,
        input_queue=input_queue,
    )

    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data_1_q1_conn_1 = {
        "transaction_id": "date_filter_test#1",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test4", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_data_1_q3_4_conn_1 = {
        "transaction_id": "date_filter_test#1",
        "conn_id": conn_id_1,
        "queries": [3, 4],
        "data": [
            {"title": "test2", "authors": ["peter"]},
        ],
    }

    expected_data_q1_conn_2 = {
        "transaction_id": "date_filter_test#2",
        "conn_id": conn_id_2,
        "queries": [1],
        "data": [
            {"title": "test5", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_data_2_q1_conn_1 = {
        "transaction_id": "date_filter_test#3",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test6", "categories": ["fiction"], "publisher": "test_pub"},
        ],
    }

    expected_eof_conn_2 = {
        "transaction_id": "date_filter_test#2_EOF",
        "conn_id": conn_id_2,
        "queries": [1, 3, 4],
        "forward_to": [output_queue_q1, output_queue_q3_4_prefix + str(conn_id_2)],
        "EOF": True,
    }

    expected_eof_conn_1 = {
        "transaction_id": "date_filter_test#3_EOF",
        "conn_id": conn_id_1,
        "queries": [1, 3, 4],
        "forward_to": [output_queue_q1, output_queue_q3_4_prefix + str(conn_id_1)],
        "EOF": True,
    }

    # Get actual results
    data_1_q1_conn_1 = messaging.get_msgs_from_queue(output_queue_q1)
    data_1_q1_conn_1_dup = messaging.get_msgs_from_queue(output_queue_q1)

    data_1_q3_4_conn_1 = messaging.get_msgs_from_queue(
        output_queue_q3_4_prefix + str(conn_id_1)
    )

    data_q1_conn_2 = messaging.get_msgs_from_queue(output_queue_q1)

    data_2_q1_conn_1 = messaging.get_msgs_from_queue(output_queue_q1)

    eof_conn_2 = messaging.get_msgs_from_queue(eof_queue)
    eof_conn_1 = messaging.get_msgs_from_queue(eof_queue)

    assert json.loads(eof_conn_2) == expected_eof_conn_2
    assert json.loads(eof_conn_1) == expected_eof_conn_1
    assert data_1_q1_conn_1_dup == data_1_q1_conn_1
    assert json.loads(data_1_q1_conn_1) == expected_data_1_q1_conn_1
    assert json.loads(data_1_q3_4_conn_1) == expected_data_1_q3_4_conn_1
    assert json.loads(data_q1_conn_2) == expected_data_q1_conn_2
    assert json.loads(data_2_q1_conn_1) == expected_data_2_q1_conn_1

    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
