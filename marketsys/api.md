* **host**
``http://test-bomber.pendanaan.com``

**login**
----
* **URL**

  `POST` `/api/login`

* **Json Params**

  ```json
  {
    "username": "admin",
    "password": "123123"
  }
  ```
* **response**
```json
     {
        "jwt": "fsgsgg"
     }
```
* **Note**
登录之后，所有的请求`headers`添加
```text
Authorization: Bearer 'jwt'
```

**reset password**
----
* **URL**

  `PATCH` `/api/reset-password`

* **Json Params**

  ```json
  {
    "old_password": "admin",
    "new_password": "123123"
  }
  ```
  