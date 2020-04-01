def packing_plugin(callback):
    def wrapper(*args, **kwargs):
        body = callback(*args, **kwargs)
        return {
            'data': body,
        }
    return wrapper
