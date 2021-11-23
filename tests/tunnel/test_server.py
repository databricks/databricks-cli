# # TODO(ML-17779): write unit / integration tests
# import socket
#
# import mock
# import pytest
# from tornado.iostream import IOStream
# from tornado.testing import bind_unused_port
#
# from databricks_cli.tunnel.server import StreamingServer
#
#
# @pytest.fixture
# def server(tunnel_config):
#     yield StreamingServer(tunnel_config)
#
#
# # Simulate client connecting to the server
# @pytest.mark.asyncio
# async def test_connect(server):
#     # mock TunnelClient
#     with mock.patch(""):
#         sock, port = bind_unused_port()
#         server.add_socket(sock)
#         client = IOStream(socket.socket())
#         await client.connect(("localhost", port))
#         await client.write(b"hello")
#         # yield client.read_until_close()
#         client.close()
#         server.stop()
#
#
# # Simulate client writing stream to the server
# @pytest.mark.asyncio
# async def test_write_stream(server):
#     sock, port = bind_unused_port()
#     server.add_socket(sock)
#     client = IOStream(socket.socket())
#     await client.connect(("localhost", port))
#     await client.write(b"hello")
#     # assert handle_stream is called
#     # yield client.read_until_close()
#     client.close()
#     server.stop()
#
#
# # Simulate client server writing stream to the client
#     # successful
#     # unsuccessful
# def test_receive_stream():
#     pass
#
#
# # simulate client disconnecting
# def test_disconnect():
#     pass
