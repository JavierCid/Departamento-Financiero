@echo off
cd /d "C:\Users\Javier Cid\source\repos\pdf-service"
start "" cmd /k "uvicorn main:app --reload --port 8000"
