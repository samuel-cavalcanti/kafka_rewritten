import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client_socket, address = server.accept()  # wait for client

    data = client_socket.recv(1028)
    print("input", data, len(data))
    #The correlation_id field is a 32-bit signed integer.
    correlation_id = 7
    # The message_size field is a 32-bit signed integer
    mensage_size = 0

    bytes_len = 4 # 32/4



    response_bytes = mensage_size.to_bytes(bytes_len) + correlation_id.to_bytes(bytes_len)

    print("output", response_bytes, len(response_bytes))
    client_socket.send(response_bytes)


if __name__ == "__main__":
    main()
