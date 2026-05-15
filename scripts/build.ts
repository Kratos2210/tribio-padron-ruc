/**
 * Descarga el Padrón Reducido RUC de SUNAT, lo parsea y lo emite como
 * `chunks/XXXX.json`, particionado por los primeros 4 dígitos del RUC.
 *
 * El padrón viene como ZIP (~368 MB) con un único TXT delimitado por `|`.
 * Lo procesamos en streaming para no cargar el archivo completo en memoria.
 *
 * Layout del padrón reducido (columnas oficiales de SUNAT):
 *   RUC | NOMBRE | ESTADO | CONDICION_DOMICILIO | UBIGEO |
 *   TIPO_VIA | NOMBRE_VIA | CODIGO_ZONA | TIPO_ZONA | NUMERO |
 *   INTERIOR | LOTE | DEPARTAMENTO | MANZANA | KILOMETRO
 *
 * Lo más útil para la API pública es: ruc, razón social, estado,
 * condición de domicilio, ubigeo y los tres niveles geográficos
 * (departamento/provincia/distrito derivados del ubigeo).
 */

import { mkdir, rm, writeFile, readFile } from "node:fs/promises";
import { createWriteStream, createReadStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import { createInterface } from "node:readline";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";

const execFileAsync = promisify(execFile);

const PADRON_URL =
  "https://www.sunat.gob.pe/descargaPRR/padron_reducido_ruc.zip";
const TMP_DIR = path.resolve("tmp");
const ZIP_PATH = path.join(TMP_DIR, "padron.zip");
const TXT_PATH = path.join(TMP_DIR, "padron.txt");
const CHUNKS_DIR = path.resolve("chunks");
const MANIFEST_PATH = path.resolve("manifest.json");

// Columnas que exportamos (estables, mismo orden en cada chunk).
const OUT_COLUMNS = [
  "ruc",
  "razon_social",
  "estado",
  "condicion",
  "tipo_contribuyente",
  "ubigeo",
  "direccion",
  "departamento",
  "provincia",
  "distrito",
] as const;

const VALID_PREFIXES = new Set(["10", "15", "16", "17", "20"]);

type Row = (string | null)[];
type Chunk = { records: Row[] };

async function main() {
  console.log("→ Preparando carpetas");
  await mkdir(TMP_DIR, { recursive: true });
  await rm(CHUNKS_DIR, { recursive: true, force: true });
  await mkdir(CHUNKS_DIR, { recursive: true });

  console.log(`→ Descargando padrón: ${PADRON_URL}`);
  await downloadFile(PADRON_URL, ZIP_PATH);

  console.log("→ Descomprimiendo");
  await execFileAsync("unzip", ["-o", ZIP_PATH, "-d", TMP_DIR]);
  // El ZIP contiene un único archivo (nombre variable). Detectarlo:
  const txtFile = await findExtractedTxt(TMP_DIR);
  if (!txtFile) throw new Error("No se encontró el TXT dentro del ZIP");

  console.log(`→ Procesando ${txtFile}`);
  const { totalRecords, prefixesCount } = await parseAndChunk(txtFile);

  const updatedAt = new Date().toISOString();
  await writeChunkFiles(updatedAt);
  await writeFile(
    MANIFEST_PATH,
    JSON.stringify(
      {
        updated_at: updatedAt,
        source: PADRON_URL,
        total_records: totalRecords,
        prefixes_count: prefixesCount,
        columns: OUT_COLUMNS,
      },
      null,
      2,
    ),
  );

  console.log(
    `✓ Listo: ${totalRecords.toLocaleString()} registros en ${prefixesCount} chunks`,
  );
}

async function downloadFile(url: string, dest: string) {
  const res = await fetch(url);
  if (!res.ok || !res.body) {
    throw new Error(`Descarga falló: HTTP ${res.status}`);
  }
  await pipeline(res.body as unknown as NodeJS.ReadableStream, createWriteStream(dest));
}

async function findExtractedTxt(dir: string): Promise<string | null> {
  const { readdir } = await import("node:fs/promises");
  const entries = await readdir(dir);
  const txt = entries.find((e) => e.toLowerCase().endsWith(".txt"));
  if (!txt) return null;
  const dst = TXT_PATH;
  if (path.join(dir, txt) !== dst) {
    await execFileAsync("mv", [path.join(dir, txt), dst]);
  }
  return dst;
}

// Acumulamos en memoria por prefijo. ~11M registros × ~10 cols pequeñas
// cabe holgadamente en 1-2 GB de RAM (el GHA runner tiene 7 GB).
const buckets = new Map<string, Chunk>();

async function parseAndChunk(file: string) {
  let totalRecords = 0;
  const stream = createReadStream(file, { encoding: "latin1" });
  const rl = createInterface({ input: stream, crlfDelay: Infinity });

  let header: string[] | null = null;
  let colIdx: Record<string, number> = {};

  for await (const raw of rl) {
    const line = raw.trim();
    if (!line) continue;
    const cols = line.split("|").map((s) => s.trim());

    if (!header) {
      header = cols.map((c) => c.toUpperCase());
      colIdx = Object.fromEntries(header.map((c, i) => [c, i]));
      continue;
    }

    const ruc = cols[colIdx.RUC] ?? "";
    if (!/^\d{11}$/.test(ruc)) continue;
    if (!VALID_PREFIXES.has(ruc.slice(0, 2))) continue;

    const prefix = ruc.slice(0, 4);
    const ubigeo = cols[colIdx.UBIGEO] ?? "";
    const geo = ubigeoToGeo(ubigeo);

    const record: Row = [
      ruc,
      cleanText(cols[colIdx.NOMBRE]),
      cleanText(cols[colIdx.ESTADO]),
      cleanText(cols[colIdx.CONDICION_DOMICILIO]),
      tipoContribuyenteFromRuc(ruc),
      ubigeo || null,
      buildDireccion(cols, colIdx),
      geo.departamento,
      geo.provincia,
      geo.distrito,
    ];

    let bucket = buckets.get(prefix);
    if (!bucket) {
      bucket = { records: [] };
      buckets.set(prefix, bucket);
    }
    bucket.records.push(record);
    totalRecords++;
  }

  return { totalRecords, prefixesCount: buckets.size };
}

async function writeChunkFiles(updatedAt: string) {
  for (const [prefix, chunk] of buckets) {
    const payload = {
      prefix,
      updated_at: updatedAt,
      columns: OUT_COLUMNS,
      records: chunk.records,
    };
    await writeFile(
      path.join(CHUNKS_DIR, `${prefix}.json`),
      JSON.stringify(payload),
    );
  }
}

function cleanText(s: string | undefined): string | null {
  if (!s) return null;
  const t = s.replace(/\s+/g, " ").trim();
  return t.length ? t : null;
}

function tipoContribuyenteFromRuc(ruc: string): string {
  // Heurística simple basada en el prefijo del RUC.
  const p = ruc.slice(0, 2);
  if (p === "10") return "PERSONA NATURAL";
  if (p === "15" || p === "17") return "PERSONA NATURAL NO DOMICILIADA";
  if (p === "16") return "SUCESION INDIVISA";
  return "PERSONA JURIDICA";
}

function buildDireccion(
  cols: string[],
  idx: Record<string, number>,
): string | null {
  const parts = [
    cols[idx.TIPO_VIA],
    cols[idx.NOMBRE_VIA],
    cols[idx.NUMERO] && `Nro. ${cols[idx.NUMERO]}`,
    cols[idx.INTERIOR] && `Int. ${cols[idx.INTERIOR]}`,
    cols[idx.MANZANA] && `Mz. ${cols[idx.MANZANA]}`,
    cols[idx.LOTE] && `Lt. ${cols[idx.LOTE]}`,
    cols[idx.KILOMETRO] && `Km. ${cols[idx.KILOMETRO]}`,
    cols[idx.TIPO_ZONA] && cols[idx.CODIGO_ZONA]
      ? `${cols[idx.TIPO_ZONA]} ${cols[idx.CODIGO_ZONA]}`
      : null,
  ].filter((p) => p && String(p).trim().length);
  if (!parts.length) return null;
  return parts.join(" ").replace(/\s+/g, " ").trim();
}

let ubigeoTable: Record<string, { departamento: string; provincia: string; distrito: string }> | null = null;

async function loadUbigeoTable() {
  // Tabla mínima embebida en `data/ubigeo.json` (opcional). Si no existe,
  // devolvemos null y los geos quedan vacíos — el ubigeo igual va en cada
  // registro y el cliente puede resolverlos por su cuenta si los necesita.
  try {
    const raw = await readFile(path.resolve("data/ubigeo.json"), "utf8");
    ubigeoTable = JSON.parse(raw);
  } catch {
    ubigeoTable = {};
  }
}

function ubigeoToGeo(code: string) {
  if (!ubigeoTable) return { departamento: null, provincia: null, distrito: null };
  const hit = ubigeoTable[code];
  if (!hit) return { departamento: null, provincia: null, distrito: null };
  return hit;
}

await loadUbigeoTable();
await main();
