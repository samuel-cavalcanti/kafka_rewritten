from pathlib import Path


def main():
    # kafka_log_2 = ("kafka.log", DATA_2)

    def write_bytes(path, data):
        log_file = Path(path)
        log_file.write_bytes(data)

    # write_bytes(*kafka_log_1)
    # write_bytes(*kafka_log_2)


# DATA_2 = b"\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00O\x00\x00\x00\x01\x02\xb0iE|\x00\x00\x00\x00\x00\x00\x00\x00\x01\x91\xe0Z\xf8\x18\x00\x00\x01\x91\xe0Z\xf8\x18\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x01:\x00\x00\x00\x01.\x01\x0c\x00\x11metadata.version\x00\x14\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x9a\x00\x00\x00\x01\x02\x86\x83/s\x00\x00\x00\x00\x00\x01\x00\x00\x01\x91\xe0[-\x15\x00\x00\x01\x91\xe0[-\x15\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x02<\x00\x00\x00\x010\x01\x02\x00\x04foo\x00\x00\x00\x00\x00\x00@\x00\x80\x00\x00\x00\x00\x00\x00\x85\x00\x00\x90\x01\x00\x00\x02\x01\x82\x01\x01\x03\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x80\x00\x00\x00\x00\x00\x00\x85\x02\x00\x00\x00\x01\x02\x00\x00\x00\x01\x01\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x02\x10\x00\x00\x00\x00\x00@\x00\x80\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x9a\x00\x00\x00\x01\x02\xc9\x10\x10\xcd\x00\x00\x00\x00\x00\x01\x00\x00\x01\x91\xe0[-\x15\x00\x00\x01\x91\xe0[-\x15\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x02<\x00\x00\x00\x010\x01\x02\x00\x04paz\x00\x00\x00\x00\x00\x00@\x00\x80\x00\x00\x00\x00\x00\x00p\x00\x00\x90\x01\x00\x00\x02\x01\x82\x01\x01\x03\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x80\x00\x00\x00\x00\x00\x00p\x02\x00\x00\x00\x01\x02\x00\x00\x00\x01\x01\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x02\x10\x00\x00\x00\x00\x00@\x00\x80\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\xe4\x00\x00\x00\x01\x02y\xdd7\xae\x00\x00\x00\x00\x00\x02\x00\x00\x01\x91\xe0[-\x15\x00\x00\x01\x91\xe0[-\x15\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x03<\x00\x00\x00\x010\x01\x02\x00\x04quz\x00\x00\x00\x00\x00\x00@\x00\x80\x00\x00\x00\x00\x00\x00h\x00\x00\x90\x01\x00\x00\x02\x01\x82\x01\x01\x03\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00@\x00\x80\x00\x00\x00\x00\x00\x00h\x02\x00\x00\x00\x01\x02\x00\x00\x00\x01\x01\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x02\x10\x00\x00\x00\x00\x00@\x00\x80\x00\x00\x00\x00\x00\x00\x01\x00\x00\x90\x01\x00\x00\x04\x01\x82\x01\x01\x03\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00@\x00\x80\x00\x00\x00\x00\x00\x00h\x02\x00\x00\x00\x01\x02\x00\x00\x00\x01\x01\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x02\x10\x00\x00\x00\x00\x00@\x00\x80\x00\x00\x00\x00\x00\x00\x01\x00\x00"


if __name__ == "__main__":
    main()
