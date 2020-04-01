from bottle import default_app

from marketsys.api.base import ServiceAPI

app = default_app()
class accountserveice(ServiceAPI):
    def get_base_url(self):
        return app.config['service.accountjava_service.base_url']

    def default_token(self):
        return app.config['service.accountjava_service.token']