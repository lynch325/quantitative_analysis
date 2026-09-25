"""WebSocket 事件层包：服务端推送事件的注册与房间广播。

由 app.create_app 在注册完蓝图后 import（见 app/__init__.py 末尾），
实际实现在 websocket_events.py；推送数据由 services/websocket_push_service.py
定时产出。事件名必须与前端 `frontend/src/pages/RtWebsocketPage.tsx` 的监听列表一致，
改名要两端同改。
"""
# WebSocket模块 