@echo off
set ASPNETCORE_URLS=http://localhost:5000
start "" "%~dp0Departamento Financiero.exe"
timeout /t 2 >nul
start http://localhost:5000
