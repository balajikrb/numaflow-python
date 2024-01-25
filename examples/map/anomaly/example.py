from pynumaflow.mapper import Messages, Message, Datum, Mapper


def my_handler(keys: list[str], datum: Datum) -> Messages:
    val = datum.value
    output_keys = keys
    output_tags = []
    _ = datum.event_time
    _ = datum.watermark
    messages = Messages()
    num = int.from_bytes(val, "little")

    if num > 90:
        output_keys = ["critical"]
        output_tags = ["critical-tag"]
    elif num > 70:
        output_keys = ["major"]
        output_tags = ["major-tag"]

    messages.append(Message(val, keys=output_keys, tags=output_tags))
    return messages


if __name__ == "__main__":
    grpc_server = Mapper(handler=my_handler)
    grpc_server.start()
