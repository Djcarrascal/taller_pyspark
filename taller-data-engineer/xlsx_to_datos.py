"""Convierte taller_data_engineer.xlsx en datos/*.csv y datos/incremental/*.jsonl
(reemplaza el ZIP que no entregaron). Solo usa la librería estándar."""
import csv, json, re, sys, zipfile, os
import xml.etree.ElementTree as ET

NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
SALIDAS = {  # hoja -> archivo
    "clientes": "datos/customers.csv",
    "productos": "datos/products.csv",
    "pedidos": "datos/orders.csv",
    "detalle_pedidos": "datos/order_items.csv",
    "pedidos_incrementales": "datos/incremental/orders_incremental.jsonl",
    "detalle_incremental": "datos/incremental/order_items_incremental.jsonl",
}

def col_idx(ref):
    letras = re.match(r"[A-Z]+", ref).group()
    n = 0
    for ch in letras:
        n = n * 26 + ord(ch) - 64
    return n - 1

def leer_hoja(z, path, shared):
    root = ET.fromstring(z.read(path))
    filas = []
    for row in root.iterfind(".//m:sheetData/m:row", NS):
        vals = {}
        for c in row.iterfind("m:c", NS):
            t = c.get("t")
            if t == "inlineStr":
                v = "".join(x.text or "" for x in c.iterfind(".//m:t", NS))
            else:
                ve = c.find("m:v", NS)
                v = "" if ve is None else ve.text or ""
                if t == "s":
                    v = shared[int(v)]
            vals[col_idx(c.get("r"))] = v
        ancho = max(vals) + 1 if vals else 0
        filas.append([vals.get(i, "") for i in range(ancho)])
    return filas

def main(xlsx):
    z = zipfile.ZipFile(xlsx)
    shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        sroot = ET.fromstring(z.read("xl/sharedStrings.xml"))
        shared = ["".join(t.text or "" for t in si.iterfind(".//m:t", NS)) for si in sroot.iterfind("m:si", NS)]
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
    rid2path = {r.get("Id"): "xl/" + r.get("Target").lstrip("/").removeprefix("xl/") for r in rels}
    RNS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
    for s in wb.iterfind(".//m:sheet", NS):
        nombre = s.get("name")
        destino = SALIDAS.get(nombre)
        if not destino:
            continue
        filas = leer_hoja(z, rid2path[s.get(RNS)], shared)
        header, datos = filas[0], filas[1:]
        datos = [f + [""] * (len(header) - len(f)) for f in datos]
        os.makedirs(os.path.dirname(destino), exist_ok=True)
        if destino.endswith(".csv"):
            with open(destino, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(header)
                w.writerows(datos)
        else:
            with open(destino, "w", encoding="utf-8") as f:
                for d in datos:
                    # vacío -> null, igual que haría un JSON real
                    f.write(json.dumps({k: (v if v != "" else None) for k, v in zip(header, d)}, ensure_ascii=False) + "\n")
        print(f"{nombre:24s} -> {destino}  ({len(datos)} filas)")

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "taller_data_engineer.xlsx")
