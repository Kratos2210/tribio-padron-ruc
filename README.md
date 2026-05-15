# tribio-padron-ruc

Padrón Reducido RUC de SUNAT particionado en chunks JSON, listo para servirse
desde un CDN como [jsdelivr](https://www.jsdelivr.com/).

Lo consume el servicio **Consulta RUC** de [Tribio](https://github.com/) sin
necesidad de subir el padrón completo (~3 GB) a una base de datos.

## Cómo funciona

1. La GitHub Action `daily-update.yml` corre cada día a las 06:00 (Lima).
2. Descarga `padron_reducido_ruc.zip` desde
   `https://www.sunat.gob.pe/descargaPRR/padron_reducido_ruc.zip`.
3. Lo descomprime y lo procesa en streaming (`scripts/build.ts`).
4. Particiona los registros por los **primeros 4 dígitos del RUC** y escribe
   un archivo `chunks/XXXX.json` por prefijo (~150–500 KB cada uno).
5. Si hubo cambios, commitea `chunks/` y `manifest.json`.

## Formato de un chunk

```json
{
  "prefix": "2010",
  "updated_at": "2026-05-15T11:00:00.000Z",
  "columns": [
    "ruc",
    "razon_social",
    "estado",
    "condicion",
    "tipo_contribuyente",
    "ubigeo",
    "direccion",
    "departamento",
    "provincia",
    "distrito"
  ],
  "records": [
    ["20100070970", "SUPERINTENDENCIA NACIONAL...", "ACTIVO", "HABIDO", "PERSONA JURIDICA", "150101", "AV. GARCILASO DE LA VEGA Nro. 1472", null, null, null]
  ]
}
```

Los registros son **arrays posicionales** (no objetos) para reducir el tamaño
del JSON ~40%. El consumidor mapea posición → nombre usando `columns`.

## Consumo

```
GET https://cdn.jsdelivr.net/gh/<owner>/tribio-padron-ruc@latest/chunks/2010.json
```

jsdelivr cachea por commit; pasa por edge global y no tiene rate limit.

## Tabla de ubigeo (opcional)

Para que `departamento`, `provincia` y `distrito` vengan resueltos hay que
agregar `data/ubigeo.json` con el formato:

```json
{
  "150101": { "departamento": "LIMA", "provincia": "LIMA", "distrito": "LIMA" }
}
```

Si el archivo no existe, los tres campos quedan `null` y el consumidor puede
resolver el ubigeo por su cuenta. El RUC + ubigeo siempre van en el chunk.

## Desarrollo local

```bash
npm run build
```

Necesita Node 22+ (`--experimental-strip-types`) y `unzip` en el PATH.
