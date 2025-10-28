@echo off
cd /d "C:\Users\Javier Cid\source\repos\Departamento Financiero\pdf-service"
python -m uvicorn main:app --host 127.0.0.1 --port 8000 --reload
pause
