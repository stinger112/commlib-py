#!/usr/bin/env python

from commlib.async_utils import async_wrap, safe_gather, wait_til
from commlib.compression import CompressionType
from commlib.msg import RPCMessage
from commlib.node import Node
from commlib.transports.mqtt import ConnectionParameters, MQTTTransport

conn_params = ConnectionParameters(
    host="localhost", port=1883, username="admin", password="admin"
)


class AddTwoIntMessage(RPCMessage):
    class Request(RPCMessage.Request):
        a: int = 0
        b: int = 0

    class Response(RPCMessage.Response):
        c: int = 0


def add_two_int_handler(msg):
    print(f"Request Message: {msg.__dict__}")
    resp = AddTwoIntMessage.Response(c=msg.a + msg.b)
    return resp


if __name__ == "__main__":
    with MQTTTransport(
        conn_params=conn_params,
        compression=CompressionType.NO_COMPRESSION,
    ) as transport:
        print('Connecting MQTT transport...')
        while True:
            if transport.is_connected:
                break
        print('MQTT transport connected.')
        node = Node(
            node_name="add_two_ints_node",
            connection_params=conn_params,
            # heartbeat_uri='nodes.add_two_ints.heartbeat',
            debug=True,
        )
        rpc = node.create_rpc(
            msg_type=AddTwoIntMessage,
            rpc_name="add_two_ints_node.add_two_ints",
            on_request=add_two_int_handler,
        )
        node.run_forever(sleep_rate=1)
