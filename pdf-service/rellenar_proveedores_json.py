import json, os, re

# Archivos
base_dir = os.path.dirname(__file__)
txt_path = os.path.join(base_dir, "proveedores.txt")
json_path = os.path.join(base_dir, "proveedores.json")

# Leer líneas del TXT
with open(txt_path, "r", encoding="utf-8") as f:
    lines = [ln.strip() for ln in f if ln.strip()]

def limpiar(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-záéíóúüñ0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

out = {}

for full in lines:
    base = limpiar(full)
    parts = base.split()

    # Añade versiones progresivas (por ejemplo "ferrovial", "ferrovial construccion")
    for i in range(1, min(4, len(parts)) + 1):
        key = " ".join(parts[:i])
        if len(key) >= 4:  # evita palabras muy cortas tipo "s.a."
            out[key] = full

# Guardar en el JSON (sobrescribe el vacío)
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=2)

print(f"✅ Rellenado {json_path} con {len(out)} palabras clave para {len(lines)} proveedores.")
