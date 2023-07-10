import asyncio
import concurrent.futures
import threading
from time import sleep

import pytest

from commlib.async_utils import async_wrap, safe_gather, wait_til
from commlib.compression import CompressionType
from commlib.msg import RPCMessage
from commlib.transports.mqtt import ConnectionParameters, MQTTTransport, RPCClient

PARALLEL_CLIENTS_N = 100

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
    with concurrent.futures.ThreadPoolExecutor(max_workers=PARALLEL_CLIENTS_N) as pool:
        with MQTTTransport(
            conn_params=conn_params,
            compression=compression,
        ) as transport:
            await wait_til(lambda: transport.is_connected)
            n = PARALLEL_CLIENTS_N
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
            resps = await asyncio.wait_for(safe_gather(*futures), timeout=10)
            print("Response numbers:", [resp.c for resp in resps])
            assert list(range(n)) == [resp.c for resp in resps]
