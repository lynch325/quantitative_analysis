@echo off
chcp 65001 >nul
title quantitative_analysis - backend :5000
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
echo  backend  http://localhost:5000
echo  python   %CD%\.venv\Scripts\python.exe
echo  stop     close this window (or Ctrl-C)
echo ============================================
.venv\Scripts\python.exe run.py
pause
