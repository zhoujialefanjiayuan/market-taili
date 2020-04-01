* **sql命令**
mysql -u ikidana -p467661568cx#CX -h rm-k1acatg07bfq009bz.mysql.ap-southeast-5.rds.aliyuncs.com

* **host**
``http://101.132.47.14:1120``
* ###所有的错误响应
```json
{
    "error": {
        "message": "Account Expired"
    }
}
```
---
**1.login**
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
---
**2.reset password**
----
* **URL**

  `POST` `/api/reset-password`

* **Json Params**

  ```json
  {
    "old_password": "admin",
    "new_password": "123123"
  }
  ```
---
**3.single_sendmessage发送单条短信**
---

* **URL**

`POST` `/api/single_sendmessage`

* **Json Params**

  ```json
  {
    "phone_number": "1234566",
    "content": "fagsbg"
  }
  ```
 * **response**
```json
{
    "data": {
        "send": "ok",
        "statu": 200
    }
}
```
---
**4.search_sendmessagelog搜索发送记录**
--
* **URL**

`POST` `/api/search_sendmessagelog?page=1&page_size=20`
* **Note**
* url后拼接，page为第几页，pagesize为每页数量
* **Json Params**

```json
{
"phone_number": "1234566",
"context": "fagsbg",
"start_senttime": "2019-11-27 18:47:11",
"end_senttime": ""
}
```
* **response**
```json
{
    "data": {
        "total_page": 1,
        "result": [
            {
                "id": "1",
                "statu": "ok",
                "created_at": "2019-11-28 18:47:11",
                "phone_number": "6281291465648",
                "content": "jugblb"
            }
        ],
        "page_size": 20,
        "page": 1,
        "total_count": 1
    }
}
```

**5.getexcel上传excel文件**
---

* **URL**

`POST` `/api/getexcel`

* **Json Params**

```json
{
"filename": "afaf.xlsx"(需带文件后缀)
"excel": file文件
}
```
 * **response**
```json
{
    "data": {
              'statu':200,
              'data':[[6281291465648 'jnkjfja'],[6281291465649 'fafjafgaf']],
              'filename':filename
            }
}
```


**6.发送表格所有短信**
---

* **URL**

`POST` `/api/send_allmessage`

* **Json Params**

```json
{
"filename": "afaf.xlsx"(需带文件后缀)
}
```
 * **response**
```json
{
    "data": {
        "statu": 200
    }
}
```
**7.通过用户id查找**
---

* **URL**

`POST` `/api/getuser-by-user_id`

* **Json Params**

```json
{
"userid": ""
}
```
 * **response**
```json
{
    "data": {
        "statu": 200
    }
}
```
**9.搜索展示用户信息**
---

* **URL**

`POST` `search_userdetil`

* **Json Params**

```json
{
"mobileno": ""
}
```
 * **response**
```json
{
    "data": {
        "statu": 200
    }
}
```

```
**8.通过手机号查找**
---

* **URL**

`POST` `/api/getuser-by-mobile_no`

* **Json Params**

```json
{
"mobileno": ""
}
```
 * **response**
```json
{
    "data": {
        "statu": 200
    }
}
```
