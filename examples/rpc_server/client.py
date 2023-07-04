#!/usr/bin/env python

import time
from commlib.transports.mqtt import ConnectionParameters
from commlib.node import Node, TransportType
from commlib.msg import RPCMessage


class AddTwoIntMessage(RPCMessage):
    class Request(RPCMessage.Request):
        a: int = 0
        b: int = 0

    class Response(RPCMessage.Response):
        c: int = 0


if __name__ == "__main__":
    conn_params = ConnectionParameters(
        host="localhost", port=1883, username="admin", password="admin"
    )
    rpc_name = "add_two_ints_node.add_two_ints"

    node = Node(
        node_name="myclient",
        connection_params=conn_params,
        # heartbeat_uri='nodes.add_two_ints.heartbeat',
        debug=True,
    )

    rpc = node.create_rpc_client(msg_type=AddTwoIntMessage, rpc_name=rpc_name)

    node.run()

    # Create an instance of the request object
    msg = AddTwoIntMessage.Request()

    while True:
        # returns AddTwoIntMessage.Response instance
        resp = rpc.call(msg)
        print(resp)
        msg.a += 1
        msg.b += 1
        time.sleep(1)
