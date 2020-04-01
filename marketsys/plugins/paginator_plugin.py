import inspect

from bottle import request


class PaginatorPlugin(object):
    def __init__(self, keyword='page'):
        self.keyword = keyword

    def apply(self, callback, route):
        _callback = route['callback']

        # Test if the original callback accepts a 'user' keyword.
        # Ignore it if it does not need a database handle.
        argspec = inspect.signature(_callback)
        if self.keyword not in argspec.parameters:
            return callback

        def wrapper(*args, **kwargs):
            page = int(request.params.get('page') or 1)
            if 'page' in request.params:
                request.query.dict.pop('page')
            kwargs[self.keyword] = page
            return callback(*args, **kwargs)

        # Replace the route callback with the wrapped one.
        return wrapper


def page_plugin(callback):
    default_page = 1
    default_page_size = 20

    def wrapper(*args, **kwargs):
        query, serializer = callback(*args, **kwargs)
        page = default_page
        page_size = default_page_size

        if request.query.page.isdigit():
            page = int(request.query.page)
        if request.query.page_size.isdigit():
            page_size = int(request.query.page_size)

        result = query.paginate(page, page_size)
        result = serializer.dump(result, many=True).data

        if len(result) < page_size:
            total_count = (page-1)*page_size + len(result)
        else:
            # todo caching
            total_count = query.count()

        return {
            'page': page,
            'page_size': page_size,
            'total_count': total_count,
            'total_page': ((total_count or 1) - 1)//page_size + 1,
            'result': result,
        }

    return wrapper
