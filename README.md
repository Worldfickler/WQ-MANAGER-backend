# WQ Manager Backend

基于 FastAPI 的后端服务，提供榜单、趋势、个人中心、Genius 仪表盘、反馈等 API。

在线体验：https://wqmanager.qzz.io/

## 技术栈

- FastAPI / Uvicorn
- SQLAlchemy (Async)
- MySQL
- Redis（可选缓存）

## 本地运行

```bash
python -m venv venv
venv\Scripts\activate       # Windows
# source venv/bin/activate  # Linux/Mac

pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

访问：
- Health: http://localhost:8000/health
- Swagger: http://localhost:8000/docs

## Docker 构建与启动

在 `backend/` 目录执行：

```bash
docker build -t wq-backend .
docker run -d --name wq-backend --env-file .env -p 8000:8000 wq-backend
```

## 环境变量

复制模板并修改：

```bash
cp .env.example .env
```

关键配置项：

```
PROJECT_NAME=WQ Manager API
VERSION=1.0.0
API_PREFIX=/api/v1
SECRET_KEY=change-me
ACCESS_TOKEN_EXPIRE_MINUTES=10080
ALGORITHM=HS256

ALLOWED_ORIGINS=["http://localhost:5173","http://127.0.0.1:5173"]

DATABASE_URL=mysql+pymysql://user:pass@host:3306/wq_manager?charset=utf8mb4
DEBUG=True

LOG_LEVEL=INFO
SQL_ECHO=False
SQL_ECHO_POOL=False
LOG_DIR=logs

REDIS_URL=redis://:password@127.0.0.1:6379/0
CACHE_EXPIRE_HOUR=14
CACHE_EXPIRE_MINUTE=0
CACHE_TIMEZONE=Asia/Shanghai
```

> `REDIS_URL` 为空时缓存自动关闭。

## 缓存策略

后端使用 **cache-aside**，统一缓存装饰器位于 `app/core/cache.py`：
- 自动根据请求路径 + query 生成缓存 key
- 默认 TTL 到达每日指定时间（默认 14:00，Asia/Shanghai）
- 登录相关接口不启用缓存

## 日志

日志目录：`backend/logs/`  
按天滚动切分：

- `app.log`：INFO / WARNING
- `error.log`：ERROR
- `console.log`：所有等级（包含控制台日志）


## 相关截图
- Login
![Login](picture/login.png)

- Home
![Home](picture/home.png)

- Genius
![Genius](picture/genius.png)

- Trend
![Trend](picture/trend.png)

- Profile
![Profile](picture/profile.png)

- Notice
![Notice](picture/notice.png)