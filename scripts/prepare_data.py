"""Download real TLC files and export unchanged records to uncompressed CSV.
Data preparation only; not the analytical Spark implementation.
Python 3.10+; pyarrow==19.0.1.

Саме цей скрипт створив CSV і manifest.json для заявки (evidence/data_preparation/).
У репозиторії запускається в контейнері:
    docker compose run --rm tools scripts/prepare_data.py
--root за замовчуванням /data (= DATA_DIR з .env, типово ./data): Parquet у /data/data,
CSV у /data/csv, manifest.json і schema.txt у /data/evidence. Порівняйте SHA-256 з
manifest.json у корені репозиторію - вони мають збігтися.
"""
import argparse
import hashlib
import json
from pathlib import Path
import urllib.request
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq


def sha256(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(4 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', type=Path, default=Path('/data'))
    ap.add_argument('--months', type=int, default=6)
    args = ap.parse_args()
    if not 1 <= args.months <= 12:
        ap.error('--months must be 1..12')
    source = args.root / 'data'
    target = args.root / 'csv'
    evidence = args.root / 'evidence'
    for folder in (source, target, evidence):
        folder.mkdir(parents=True, exist_ok=True)
    records = []
    first_schema = None
    for month in range(1, args.months + 1):
        name = f'yellow_tripdata_2024-{month:02}.parquet'
        url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' + name
        path = source / name
        if not path.exists():
            temporary = path.with_suffix('.download')
            print('Downloading', name, flush=True)
            urllib.request.urlretrieve(url, temporary)
            temporary.replace(path)
        pf = pq.ParquetFile(path)
        schema = pf.schema_arrow.remove_metadata()
        if first_schema is None:
            first_schema = schema
            (evidence / 'schema.txt').write_text(str(schema), encoding='utf-8')
        if not schema.equals(first_schema):
            raise RuntimeError(f'Schema changed in {name}; inspect before continuing')
        out = target / path.with_suffix('.csv').name
        partial = out.with_suffix('.csv.partial')
        rows = 0
        # Own the output handle explicitly: Windows cannot rename an open file.
        with partial.open('wb') as output:
            with csv.CSVWriter(output, schema) as writer:
                for batch in pf.iter_batches(batch_size=65536):
                    writer.write_batch(batch)
                    rows += batch.num_rows
        partial.replace(out)
        assert rows == pf.metadata.num_rows
        item = dict(file=name, url=url, rows=rows, parquet_bytes=path.stat().st_size,
                    parquet_uncompressed_encoded_bytes=sum(pf.metadata.row_group(i).total_byte_size for i in range(pf.metadata.num_row_groups)),
                    parquet_sha256=sha256(path), csv_file=out.name,
                    csv_bytes=out.stat().st_size, csv_sha256=sha256(out))
        records.append(item)
        print(json.dumps(item), flush=True)
    result = dict(year=2024, months=args.months, pyarrow_version=pa.__version__,
                  csv_definition='UTF-8, uncompressed, all original columns and rows, header per monthly file; pyarrow.csv defaults',
                  files=records, total_rows=sum(r['rows'] for r in records),
                  total_parquet_bytes=sum(r['parquet_bytes'] for r in records),
                  total_csv_bytes=sum(r['csv_bytes'] for r in records))
    result['passes_10m_and_2gib'] = result['total_rows'] >= 10_000_000 and result['total_csv_bytes'] >= 2 * 1024**3
    (evidence / 'manifest.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
    print('TOTAL', json.dumps({k:v for k,v in result.items() if k != 'files'}), flush=True)
    if not result['passes_10m_and_2gib']:
        raise SystemExit('Threshold not met: add a month; do not submit yet.')

if __name__ == '__main__':
    main()
