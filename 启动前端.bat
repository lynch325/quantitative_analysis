@echo off
chcp 65001 >nul
title quantitative_analysis - frontend :5173
cd /d %~dp0frontend
if not exist node_modules (
  echo installing frontend deps...
  call npm install --registry=https://registry.npmmirror.com --no-fund --no-audit
)
echo ============================================
echo  frontend  http://localhost:5173
echo  stop      close this window (or Ctrl-C)
echo ============================================
call npm run dev
pause
