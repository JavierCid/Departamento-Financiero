# Departamento Financiero

## Local
- Frontend (Blazor WASM): http://localhost:7252
- Functions .NET 8 (isolated): http://localhost:7130
- Endpoint: POST /api/ProcesarExcel

## Notas
- Subida: bytes (application/octet-stream), cabecera X-File-Name.
- Devuelve Excel + cabecera X-Preview para vista previa.
- CORS dinámico (Allow-Origin + Vary).
