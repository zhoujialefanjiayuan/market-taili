import json

from marshmallow import Schema
from marshmallow import fields


class JsonField(fields.Field):
    def _serialize(self, value, attr, obj):
        return json.loads(value or '[]')



class BaseSchema(Schema):
    created_at = fields.DateTime('%Y-%m-%d %H:%M:%S')



class sendmessagelog_serializer(BaseSchema):
    id = fields.Int(as_string=True)
    phone_number = fields.Str()
    content = fields.Str()
    statu = fields.Str()



sendmessagelog_serializer = sendmessagelog_serializer(strict=True)
