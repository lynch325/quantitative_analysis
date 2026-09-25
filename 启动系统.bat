@echo off
chcp 65001 >nul
title quantitative_analysis launcher
cd /d %~dp0
start "qa-backend-5000" cmd /k "%~dp0启动后端.bat"
echo waiting backend boot (8s)...
timeout /t 8 >nul
start "qa-frontend-5173" cmd /k "%~dp0启动前端.bat"
echo.
echo ============================================
echo  打开浏览器访问:  http://localhost:5173
echo  API 文档/根地址:  http://localhost:5000
echo  关闭: 两个黑色窗口分别 Ctrl-C 或直接关窗口
echo ============================================
pause
