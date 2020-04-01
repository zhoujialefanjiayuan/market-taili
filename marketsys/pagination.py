from marshmallow import Schema


def paging(query, serializer, page, page_size=20):
    page_query = query.paginate(page, page_size)
    if isinstance(serializer, Schema):
        result = serializer.dump(page_query, many=True).data
    elif callable(serializer):
        result = serializer()
    else:
        raise TypeError

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
