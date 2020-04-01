import logging
import time
from bottle import request, abort
from functools import wraps


def boilerplate_plugin(callback):
    def wrapper(*args, **kwargs):
        body = callback(*args, **kwargs)
        return {
            'data': body,
        }

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
            total_count = len(result)
        else:
            # todo caching
            total_count = query.count()

        return {
            'page': page,
            'page_size': page_size,
            'total_count': total_count,
            'total_page': ((total_count or 1) - 1) // page_size + 1,
            'result': result,
        }

    return wrapper


def logging_plugin(callback):
    def wrapper(*args, **kwargs):
        content_type = request.headers.get('CONTENT_TYPE')
        if content_type == 'image/jpeg':
            params = 'image/jpeg'
        else:
            params = request.body.read()
        func_name = callback.__name__
        start = time.time()
        status = 'success'
        body = None
        try:
            body = callback(*args, **kwargs)
        except Exception as e:
            body = type(e)
            status = 'error'
            raise e
        finally:
            end = time.time()
            time_diff = round((end - start) * 1000, 3)
            if status == 'success':
                logging.info(
                    'request details: '
                    'func_name:%s, status:%s, time_diff:%sms, params:%s, '
                    'args:%s, kwargs: %s, body: %s',
                    func_name, status, time_diff, params,
                    args, kwargs, body
                )
            else:
                logging.error(
                    'request details: '
                    'func_name:%s, status:%s, time_diff:%sms, params:%s, '
                    'args:%s, kwargs: %s, body: %s',
                    func_name, status, time_diff, params,
                    args, kwargs, body
                )
        return body

    return wrapper


class PretreatmentPlugin(object):
    def __init__(self, url, method):
        self.url = url
        self.method = method

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            service = args[0]
            method_lower = self.method.lower()
            method = getattr(service, method_lower)
            logging.info('request url {}, params {}'.format(self.url, kwargs))
            if method_lower == 'post':
                # 根据files字段判断post请求是不是上传文件
                if 'files' in kwargs:
                    resp = method(self.url, files=kwargs['files'])
                else:
                    resp = method(self.url, json=kwargs)
            else:
                if '{' in self.url:
                    url = self.url.format(resource_id=kwargs['resource_id'])
                else:
                    url = self.url
                if method_lower == 'delete':
                    resp = method(url)
                else:
                    resp = method(url, kwargs)
            if not resp.ok:
                if hasattr(resp, 'json'):
                    body = resp.json()
                    logging.exception('request has exception code: {} msg: {}'
                                      .format(body.get('code',500),
                                              body.get('chineseMessage','')))
                    abort(resp.status_code, body.get('message',""))
                else:
                    abort(resp.status_code)
            else:
                if hasattr(resp, 'json'):
                    if isinstance(resp.json(), dict):
                        data = resp.json().get('data', resp.json())
                    else:
                        data = resp.json()
                else:
                    return None
                logging.info('response data {}'.format(data))
                return data

        return wrapper

    def format_params(self, **kwargs):
        formatted_params = {}
        for k, v in kwargs.items():
            formatted_params[self.camel_to_underline(k)] = v
        return formatted_params

    @staticmethod
    def camel_to_underline(camel_format):
        """
        camelToUnderline --> camel_to_underline
        :param camel_format:str
        :return:
        """
        underline_format = ''
        if isinstance(camel_format, str):
            for _s_ in camel_format:
                underline_format += _s_ if _s_.islower() else '_' + _s_.lower()
        return underline_format

    @staticmethod
    def underline_to_camel(underline_format):
        """
        underline_to_camel --> UnderlineToCamel
        :param underline_format:str
        :return:
        """
        camel_format = ''
        if isinstance(underline_format, str):
            for _s_ in underline_format.split('_'):
                camel_format += _s_.capitalize()
        return camel_format
