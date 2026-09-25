@echo off
chcp 65001 >nul
title quantitative_analysis - single process (frontend + backend :5000)
cd /d %~dp0
if not exist .venv\Scripts\python.exe (
  echo [ERROR] .venv not found. Run: uv venv .venv --python D:\Anaconda\python.exe
  pause
  exit /b 1
)
rem run.py 会打印 ⚠/中文，GBK 控制台会 UnicodeEncodeError，必须强制 UTF-8
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
echo ============================================
echo  frontend + backend   http://127.0.0.1:5000
echo  stop                 close this window (or Ctrl-C)
echo ============================================
echo.
echo 说明: 前端已由 Flask 托管（frontend/dist），无需另开前端窗口。
echo       改前端源码时才需要: 启动系统.bat（Vite 热更新, :5173）
echo.
rem 等后端就绪再打开浏览器，避免打开空白页
start "" cmd /c "timeout /t 10 >nul & start "" http://127.0.0.1:5000"
.venv\Scripts\python.exe run.py
pause
