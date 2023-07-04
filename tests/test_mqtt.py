import asyncio
import concurrent.futures
import threading
from time import sleep

import pytest

from commlib.async_utils import safe_gather, async_wrap
from commlib.compression import CompressionType
from commlib.msg import RPCMessage
from commlib.transports.mqtt import ConnectionParameters, MQTTTransport, RPCClient

conn_params = ConnectionParameters(
    host="localhost", port=1883, username="admin", password="admin"
)
compression = CompressionType.NO_COMPRESSION


class AddTwoIntMessage(RPCMessage):
    class Request(RPCMessage.Request):
        a: int = 0
        b: int = 0

    class Response(RPCMessage.Response):
        c: int = 0


@pytest.mark.asyncio
async def test_multiple_clients():
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        with MQTTTransport(
            conn_params=conn_params,
            compression=compression,
        ) as transport:
            while transport.is_connected is not True:
                sleep(0.1)
            assert transport.is_connected is True
            n = 100
            clients = []
            for i in range(n):
                client = RPCClient(
                    conn_params=conn_params,
                    compression=compression,
                    msg_type=AddTwoIntMessage,
                    rpc_name="add_two_ints_node.add_two_ints",
                )
                client.call_in_loop = async_wrap(client.call)
                clients.append(client)
            futures = []
            for i, client in enumerate(clients):
                # resp = client.call(AddTwoIntMessage.Request(a=i, b=0), timeout=5)
                future = client.call_in_loop(
                    AddTwoIntMessage.Request(a=i, b=0), timeout=5, executor=pool
                )
                futures.append(future)
            resps = await safe_gather(*futures)
            print("Numbers:", [resp.c for resp in resps])
            assert list(range(n)) == [resp.c for resp in resps]
