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
from src.controllers.filters.title_filter.main import TitleFilter
from src.exceptions.shutting_down import ShuttingDown
from src.messaging.message import Message
from test.mocks.mock_messaging import MockMessaging, ProvokedError

def test_category_filter_works_if_no_faults():
    controller_id = "category_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"
    filter_number = 67
    conn_id = 99

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

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body_q1 = {
        "transaction_id": "test#1",
        "queries": [1],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test1",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat5"], "publisher": "test_pub"},
            {
                "title": "superman2",
                "categories": ["test_cat1"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    msg_body_q5 = {
        "transaction_id": "test#2",
        "queries": [5],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test1",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat5"], "publisher": "test_pub"},
            {
                "title": "superman2",
                "categories": ["test_cat1"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    # Mock Messaging Server to work as IPC

    input_queue = "category_filter" + str(filter_number)
    output_queue_q1_prefix = "output1_category_filter_test"
    output_queue_q5_prefix = "output5_category_filter_test"
    eof_queue = "eof_category_filter_test"

    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=[
            "output1_category_filter_test" + str(conn_id),
            "output5_category_filter_test" + str(conn_id),
            "eof_category_filter_test",
        ],
        msgs_to_consume=2,
    )

    messaging.send_to_queue(input_queue, Message(msg_body_q1))
    messaging.send_to_queue(input_queue, Message(msg_body_q5))

    # Start the filter
    filter = CategoryFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        category_q1="test_cat1",
        eof_queue=eof_queue,
        output_queue_q1_prefix=output_queue_q1_prefix,
        output_queue_q5_prefix=output_queue_q5_prefix,
        category_q5="test_cat5",
        input_queue=input_queue,
    )

    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data_q1 = {
        "transaction_id": "category_filter_test#1",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "publisher": "test_pub"},
            {"title": "superman2", "publisher": "test_pub"},
        ],
    }

    expected_data_q5 = {
        "transaction_id": "category_filter_test#2",
        "conn_id": conn_id,
        "queries": [5],
        "data": [
            {
                "title": "test3",
            },
        ],
    }

    expected_eof_q1 = {
        "transaction_id": "category_filter_test#1_EOF",
        "conn_id": conn_id,
        "EOF": True,
        "forward_to": [output_queue_q1_prefix + str(conn_id)],
        "queries": [1],
    }

    expected_eof_q5 = {
        "transaction_id": "category_filter_test#2_EOF",
        "conn_id": conn_id,
        "EOF": True,
        "forward_to": [output_queue_q5_prefix + str(conn_id)],
        "queries": [5],
    }

    # Get actual results
    data_q1 = messaging.get_msgs_from_queue(
        "output1_category_filter_test" + str(conn_id)
    )
    data_q5 = messaging.get_msgs_from_queue(
        "output5_category_filter_test" + str(conn_id)
    )
    data = [data_q1, data_q5]

    eof_1 = messaging.get_msgs_from_queue("eof_category_filter_test")
    eof_2 = messaging.get_msgs_from_queue("eof_category_filter_test")
    eof = [eof_1, eof_2]

    assert json.dumps(expected_eof_q1) in eof
    assert json.dumps(expected_eof_q5) in eof
    assert json.dumps(expected_data_q1) in data
    assert json.dumps(expected_data_q5) in data


    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)


def test_category_filter_ignores_duplicate_transactions():
    controller_id = "category_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"
    filter_number = 67
    conn_id = 99

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

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body_q1 = {
        "transaction_id": "test#1",
        "queries": [1],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test1",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat5"], "publisher": "test_pub"},
            {
                "title": "superman2",
                "categories": ["test_cat1"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    msg_body_q5 = {
        "transaction_id": "test#2",
        "queries": [5],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test1",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat5"], "publisher": "test_pub"},
            {
                "title": "superman2",
                "categories": ["test_cat1"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    # Mock Messaging Server to work as IPC

    input_queue = "category_filter" + str(filter_number)
    output_queue_q1_prefix = "output1_category_filter_test"
    output_queue_q5_prefix = "output5_category_filter_test"
    eof_queue = "eof_category_filter_test"

    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=[
            "output1_category_filter_test" + str(conn_id),
            "output5_category_filter_test" + str(conn_id),
            "eof_category_filter_test",
        ],
        msgs_to_consume=4,
    )

    messaging.send_to_queue(input_queue, Message(msg_body_q1))
    messaging.send_to_queue(input_queue, Message(msg_body_q1))
    messaging.send_to_queue(input_queue, Message(msg_body_q5))
    messaging.send_to_queue(input_queue, Message(msg_body_q5))

    # Start the filter
    filter = CategoryFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        category_q1="test_cat1",
        eof_queue=eof_queue,
        output_queue_q1_prefix=output_queue_q1_prefix,
        output_queue_q5_prefix=output_queue_q5_prefix,
        category_q5="test_cat5",
        input_queue=input_queue,
    )

    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data_q1 = {
        "transaction_id": "category_filter_test#1",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "publisher": "test_pub"},
            {"title": "superman2", "publisher": "test_pub"},
        ],
    }

    expected_data_q5 = {
        "transaction_id": "category_filter_test#2",
        "conn_id": conn_id,
        "queries": [5],
        "data": [
            {
                "title": "test3",
            },
        ],
    }

    expected_eof_q1 = {
        "transaction_id": "category_filter_test#1_EOF",
        "conn_id": conn_id,
        "EOF": True,
        "forward_to": [output_queue_q1_prefix + str(conn_id)],
        "queries": [1],
    }

    expected_eof_q5 = {
        "transaction_id": "category_filter_test#2_EOF",
        "conn_id": conn_id,
        "EOF": True,
        "forward_to": [output_queue_q5_prefix + str(conn_id)],
        "queries": [5],
    }

    # Get actual results
    data_q1 = messaging.get_msgs_from_queue(
        "output1_category_filter_test" + str(conn_id)
    )
    data_q5 = messaging.get_msgs_from_queue(
        "output5_category_filter_test" + str(conn_id)
    )
    data = [data_q1, data_q5]

    eof_1 = messaging.get_msgs_from_queue("eof_category_filter_test")
    eof_2 = messaging.get_msgs_from_queue("eof_category_filter_test")
    eof = [eof_1, eof_2]

    assert json.dumps(expected_eof_q1) in eof
    assert json.dumps(expected_eof_q5) in eof
    assert json.dumps(expected_data_q1) in data
    assert json.dumps(expected_data_q5) in data

    # Clean up
    time.sleep(0.1)

    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)


def test_category_filter_works_if_no_faults_multiple_messages():
    controller_id = "category_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"
    filter_number = 67
    conn_id = 99

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

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body_q1_1 = {
        "transaction_id": "test#1",
        "queries": [1],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test1",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test09", "categories": ["test_cat5"], "publisher": "test_pub"},
            {
                "title": "superman2",
                "categories": ["test_cat1"],
                "publisher": "test_pub",
            },
        ],
    }

    msg_body_q1_2 = {
        "transaction_id": "test#2",
        "queries": [1],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test33",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test21", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test30", "categories": ["test_cat5"], "publisher": "test_pub"},
            {"title": "batman2", "categories": ["test_cat1"], "publisher": "test_pub"},
        ],
        "EOF": True,
    }

    msg_body_q5_1 = {
        "transaction_id": "test#3",
        "queries": [5],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test178",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat5"], "publisher": "test_pub"},
            {
                "title": "superman2",
                "categories": ["test_cat1"],
                "publisher": "test_pub",
            },
        ],
    }

    msg_body_q5_2 = {
        "transaction_id": "test#4",
        "queries": [5],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test76",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test21", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test33", "categories": ["test_cat5"], "publisher": "test_pub"},
            {
                "title": "batman begins",
                "categories": ["test_cat1"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    # Mock Messaging Server to work as IPC

    input_queue = "category_filter" + str(filter_number)
    output_queue_q1_prefix = "output1_category_filter_test"
    output_queue_q5_prefix = "output5_category_filter_test"
    eof_queue = "eof_category_filter_test"

    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=[
            "output1_category_filter_test" + str(conn_id),
            "output5_category_filter_test" + str(conn_id),
            "eof_category_filter_test",
        ],
        msgs_to_consume=4,
    )

    messaging.send_to_queue(input_queue, Message(msg_body_q1_1))
    messaging.send_to_queue(input_queue, Message(msg_body_q5_1))
    messaging.send_to_queue(input_queue, Message(msg_body_q1_2))
    messaging.send_to_queue(input_queue, Message(msg_body_q5_2))

    # Start the filter
    filter = CategoryFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        category_q1="test_cat1",
        eof_queue=eof_queue,
        output_queue_q1_prefix=output_queue_q1_prefix,
        output_queue_q5_prefix=output_queue_q5_prefix,
        category_q5="test_cat5",
        input_queue=input_queue,
    )

    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data_q1_1 = {
        "transaction_id": "category_filter_test#1",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "publisher": "test_pub"},
            {"title": "superman2", "publisher": "test_pub"},
        ],
    }

    expected_data_q1_2 = {
        "transaction_id": "category_filter_test#3",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test33", "publisher": "test_pub"},
            {"title": "batman2", "publisher": "test_pub"},
        ],
    }

    expected_data_q5_1 = {
        "transaction_id": "category_filter_test#2",
        "conn_id": conn_id,
        "queries": [5],
        "data": [
            {
                "title": "test3",
            },
        ],
    }

    expected_data_q5_2 = {
        "transaction_id": "category_filter_test#4",
        "conn_id": conn_id,
        "queries": [5],
        "data": [
            {
                "title": "test33",
            },
        ],
    }

    expected_eof_q1 = {
        "transaction_id": "category_filter_test#3_EOF",
        "conn_id": conn_id,
        "EOF": True,
        "forward_to": [output_queue_q1_prefix + str(conn_id)],
        "queries": [1],
    }

    expected_eof_q5 = {
        "transaction_id": "category_filter_test#4_EOF",
        "conn_id": conn_id,
        "EOF": True,
        "forward_to": [output_queue_q5_prefix + str(conn_id)],
        "queries": [5],
    }

    # Get actual results

    data_q1_1 = messaging.get_msgs_from_queue(
        "output1_category_filter_test" + str(conn_id)
    )
    assert json.loads(data_q1_1) == expected_data_q1_1

    data_q5_1 = messaging.get_msgs_from_queue(
        "output5_category_filter_test" + str(conn_id)
    )
    assert json.loads(data_q5_1) == expected_data_q5_1

    data_q1_2 = messaging.get_msgs_from_queue(
        "output1_category_filter_test" + str(conn_id)
    )
    assert json.loads(data_q1_2) == expected_data_q1_2

    data_q5_2 = messaging.get_msgs_from_queue(
        "output5_category_filter_test" + str(conn_id)
    )
    assert json.loads(data_q5_2) == expected_data_q5_2

    eof_q1 = messaging.get_msgs_from_queue("eof_category_filter_test")
    eof_q5 = messaging.get_msgs_from_queue("eof_category_filter_test")

    assert expected_eof_q1 == json.loads(eof_q1)
    assert expected_eof_q5 == json.loads(eof_q5)

    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)


def test_category_filter_works_if_no_faults_multiple_messages_and_connections():
    controller_id = "category_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"
    filter_number = 67
    conn_id_1 = 99
    conn_id_2 = 100

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

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body_q1_conn_1 = {
        "transaction_id": "test#1",
        "queries": [1],
        "conn_id": conn_id_1,
        "data": [
            {
                "title": "test1",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test09", "categories": ["test_cat5"], "publisher": "test_pub"},
            {
                "title": "superman2",
                "categories": ["test_cat1"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    msg_body_q1_conn_2 = {
        "transaction_id": "test#2",
        "queries": [1],
        "conn_id": conn_id_2,
        "data": [
            {
                "title": "test33",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test21", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test30", "categories": ["test_cat5"], "publisher": "test_pub"},
            {"title": "batman2", "categories": ["test_cat1"], "publisher": "test_pub"},
        ],
        "EOF": True,
    }

    msg_body_q5_conn_2_1 = {
        "transaction_id": "test#3",
        "queries": [5],
        "conn_id": conn_id_2,
        "data": [
            {
                "title": "test178",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat5"], "publisher": "test_pub"},
            {
                "title": "superman2",
                "categories": ["test_cat1"],
                "publisher": "test_pub",
            },
        ],
    }

    msg_body_q5_conn_2_2 = {
        "transaction_id": "test#4",
        "queries": [5],
        "conn_id": conn_id_2,
        "data": [
            {
                "title": "test76",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test21", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test33", "categories": ["test_cat5"], "publisher": "test_pub"},
            {
                "title": "batman begins",
                "categories": ["test_cat1"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    # Mock Messaging Server to work as IPC

    input_queue = "category_filter" + str(filter_number)
    output_queue_q1_prefix = "output1_category_filter_test"
    output_queue_q5_prefix = "output5_category_filter_test"
    eof_queue = "eof_category_filter_test"

    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=[
            "output1_category_filter_test" + str(conn_id_1),
            "output5_category_filter_test" + str(conn_id_1),
            "output1_category_filter_test" + str(conn_id_2),
            "output5_category_filter_test" + str(conn_id_2),
            "eof_category_filter_test",
        ],
        msgs_to_consume=4,
    )

    messaging.send_to_queue(input_queue, Message(msg_body_q1_conn_1))
    messaging.send_to_queue(input_queue, Message(msg_body_q5_conn_2_1))
    messaging.send_to_queue(input_queue, Message(msg_body_q1_conn_2))
    messaging.send_to_queue(input_queue, Message(msg_body_q5_conn_2_2))

    # Start the filter
    filter = CategoryFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        category_q1="test_cat1",
        eof_queue=eof_queue,
        output_queue_q1_prefix=output_queue_q1_prefix,
        output_queue_q5_prefix=output_queue_q5_prefix,
        category_q5="test_cat5",
        input_queue=input_queue,
    )

    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data_q1_conn1 = {
        "transaction_id": "category_filter_test#1",
        "conn_id": conn_id_1,
        "queries": [1],
        "data": [
            {"title": "test1", "publisher": "test_pub"},
            {"title": "superman2", "publisher": "test_pub"},
        ],
    }

    expected_data_q1_conn2 = {
        "transaction_id": "category_filter_test#3",
        "conn_id": conn_id_2,
        "queries": [1],
        "data": [
            {"title": "test33", "publisher": "test_pub"},
            {"title": "batman2", "publisher": "test_pub"},
        ],
    }

    expected_data_q5_conn2_1 = {
        "transaction_id": "category_filter_test#2",
        "conn_id": conn_id_2,
        "queries": [5],
        "data": [
            {
                "title": "test3",
            },
        ],
    }

    expected_data_q5_conn2_2 = {
        "transaction_id": "category_filter_test#4",
        "conn_id": conn_id_2,
        "queries": [5],
        "data": [
            {
                "title": "test33",
            },
        ],
    }

    expected_eof_q1_conn1 = {
        "transaction_id": "category_filter_test#1_EOF",
        "conn_id": conn_id_1,
        "EOF": True,
        "forward_to": [output_queue_q1_prefix + str(conn_id_1)],
        "queries": [1],
    }

    expected_eof_q1_conn2 = {
        "transaction_id": "category_filter_test#3_EOF",
        "conn_id": conn_id_2,
        "EOF": True,
        "forward_to": [output_queue_q1_prefix + str(conn_id_2)],
        "queries": [1],
    }

    expected_eof_q5_conn2 = {
        "transaction_id": "category_filter_test#4_EOF",
        "conn_id": conn_id_2,
        "EOF": True,
        "forward_to": [output_queue_q5_prefix + str(conn_id_2)],
        "queries": [5],
    }

    # Get actual results
    data_q1_conn1 = messaging.get_msgs_from_queue(
        "output1_category_filter_test" + str(conn_id_1)
    )
    assert json.loads(data_q1_conn1) == expected_data_q1_conn1

    data_q1_conn2 = messaging.get_msgs_from_queue(
        "output1_category_filter_test" + str(conn_id_2)
    )
    assert json.loads(data_q1_conn2) == expected_data_q1_conn2

    data_q5_conn2_1 = messaging.get_msgs_from_queue(
        "output5_category_filter_test" + str(conn_id_2)
    )
    assert json.loads(data_q5_conn2_1) == expected_data_q5_conn2_1

    data_q5_conn2_2 = messaging.get_msgs_from_queue(
        "output5_category_filter_test" + str(conn_id_2)
    )
    assert json.loads(data_q5_conn2_2) == expected_data_q5_conn2_2

    eof_q1_conn1 = messaging.get_msgs_from_queue("eof_category_filter_test")
    eof_q1_conn2 = messaging.get_msgs_from_queue("eof_category_filter_test")
    eof_q5_conn2 = messaging.get_msgs_from_queue("eof_category_filter_test")

    assert expected_eof_q1_conn1 == json.loads(eof_q1_conn1)
    assert expected_eof_q1_conn2 == json.loads(eof_q1_conn2)
    assert expected_eof_q5_conn2 == json.loads(eof_q5_conn2)

    time.sleep(0.1)
    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)


@pytest.mark.parametrize("message_to_fail", [4])
def test_title_filter_recovers_from_crash_sending_data(message_to_fail: int):
    controller_id = "category_filter_test"
    file_path = f"./test/state_{controller_id}.json"
    temp_file_path = f"./test/state_{controller_id}.tmp"
    filter_number = 67
    conn_id = 99

    # Set up
    extra_fields = {
        "filtered_books": [],
        "conn_id": 0,
        "queries": [],
        "EOF": False,
    }

    config = {
        "ITEMS_PER_BATCH": 4,
        "FILTER_NUMBER": filter_number,
    }

    # Mock message to be sent to the filter
    msg_body_q1 = {
        "transaction_id": "test#1",
        "queries": [1],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test1",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat5"], "publisher": "test_pub"},
            {
                "title": "superman2",
                "categories": ["test_cat1"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    msg_body_q5 = {
        "transaction_id": "test#2",
        "queries": [5],
        "conn_id": conn_id,
        "data": [
            {
                "title": "test1",
                "categories": ["test_cat1", "computers"],
                "publisher": "test_pub",
            },
            {"title": "test2", "categories": ["fiction"], "publisher": "test_pub"},
            {"title": "test3", "categories": ["test_cat5"], "publisher": "test_pub"},
            {
                "title": "superman2",
                "categories": ["test_cat1"],
                "publisher": "test_pub",
            },
        ],
        "EOF": True,
    }

    # Mock Messaging Server to work as IPC

    input_queue = "category_filter" + str(filter_number)
    output_queue_q1_prefix = "output1_category_filter_test"
    output_queue_q5_prefix = "output5_category_filter_test"
    eof_queue = "eof_category_filter_test"

    messaging = MockMessaging(
        "localhost",
        5672,
        queues_to_export=[
            "output1_category_filter_test" + str(conn_id),
            "output5_category_filter_test" + str(conn_id),
            "eof_category_filter_test",
        ],
        msgs_to_consume=2,
        crash_on_send=message_to_fail,
    )

    messaging.send_to_queue(input_queue, Message(msg_body_q1))
    messaging.send_to_queue(input_queue, Message(msg_body_q5))

    # Start the filter
    state = ControllerState(
        controller_id=controller_id,
        file_path=file_path,
        temp_file_path=temp_file_path,
        extra_fields=extra_fields,
    )

    filter = CategoryFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        category_q1="test_cat1",
        eof_queue=eof_queue,
        output_queue_q1_prefix=output_queue_q1_prefix,
        output_queue_q5_prefix=output_queue_q5_prefix,
        category_q5="test_cat5",
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

    assert os.path.exists(file_path)
    state.update_from_file()

    filter = CategoryFilter(
        state=state,
        filter_config=config,  # type: ignore
        messaging=messaging,  # type: ignore
        category_q1="test_cat1",
        eof_queue=eof_queue,
        output_queue_q1_prefix=output_queue_q1_prefix,
        output_queue_q5_prefix=output_queue_q5_prefix,
        category_q5="test_cat5",
        input_queue=input_queue,
    )

    threading.Thread(target=filter.start).start()

    # Define expected results
    expected_data_q1 = {
        "transaction_id": "category_filter_test#1",
        "conn_id": conn_id,
        "queries": [1],
        "data": [
            {"title": "test1", "publisher": "test_pub"},
            {"title": "superman2", "publisher": "test_pub"},
        ],
    }

    expected_data_q5 = {
        "transaction_id": "category_filter_test#2",
        "conn_id": conn_id,
        "queries": [5],
        "data": [
            {
                "title": "test3",
            },
        ],
    }

    expected_eof_q1 = {
        "transaction_id": "category_filter_test#1_EOF",
        "conn_id": conn_id,
        "EOF": True,
        "forward_to": [output_queue_q1_prefix + str(conn_id)],
        "queries": [1],
    }

    expected_eof_q5 = {
        "transaction_id": "category_filter_test#2_EOF",
        "conn_id": conn_id,
        "EOF": True,
        "forward_to": [output_queue_q5_prefix + str(conn_id)],
        "queries": [5],
    }

    # Get actual results
    data_q1 = messaging.get_msgs_from_queue(
        "output1_category_filter_test" + str(conn_id)
    )
    data_q5 = messaging.get_msgs_from_queue(
        "output5_category_filter_test" + str(conn_id)
    )
    data = [data_q1, data_q5]

    eof_1 = messaging.get_msgs_from_queue("eof_category_filter_test")
    eof_2 = messaging.get_msgs_from_queue("eof_category_filter_test")
    eof = [eof_1, eof_2]

    assert json.dumps(expected_eof_q1) in eof
    assert json.dumps(expected_eof_q5) in eof
    assert json.dumps(expected_data_q1) in data
    assert json.dumps(expected_data_q5) in data

    time.sleep(0.1)

    # Clean up
    if os.path.exists(file_path):
        os.remove(file_path)
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
