import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert';
import {describe, it} from 'node:test';
import {compressors} from 'hyparquet-compressors'

import {ParquetReader, ParquetGroupReader} from '../src/index';

const fixtures = path.join(import.meta.dirname, 'fixtures');
const files = fs.readdirSync(fixtures).filter(f => f.endsWith('.parquet'))
                                      .map(f => f.replace('.parquet', ''));

describe('ParquetReader', () => {
  describe('read all data', () => {
    for (const file of files) {
      it(file, async () => {
        const json = JSON.parse(fs.readFileSync(path.join(fixtures, `${file}.json`)));
        const reader = new ParquetReader(path.join(fixtures, `${file}.parquet`));
        const rows = [];
        for await (const data of await reader.getIterator({compressors})) {
          rows.push(data);
        }
        await reader.close();
        replaceNonJsonValues(rows);
        assert.deepStrictEqual(json, rows);
      });
    }
  });

  describe('no miss in shuffle', () => {
    for (const file of files) {
      it(file, async () => {
        const json = JSON.parse(fs.readFileSync(path.join(fixtures, `${file}.json`)));
        const rowsCount = json.length;
        const reader = new ParquetReader(path.join(fixtures, `${file}.parquet`));
        let readCount = 0;
        for await (const data of await reader.getIterator({shuffle: true, compressors})) {
          readCount++;
        }
        await reader.close();
        assert.deepEqual(rowsCount, readCount);
      });
    }
  });
});

describe('ParquetGroupReader', () => {
  it('read all data', async () => {
    let json = [];
    for (const file of files) {
      const data = JSON.parse(fs.readFileSync(path.join(fixtures, `${file}.json`)));
      json = json.concat(data);
    }
    const reader = new ParquetGroupReader(files.map(file => path.join(fixtures, `${file}.parquet`)));
    const rows = [];
    for await (const data of await reader.getIterator({compressors})) {
      rows.push(data);
    }
    await reader.close();
    replaceNonJsonValues(rows);
    assert.deepStrictEqual(json, rows);
  });

  it('no miss in shuffle', async () => {
    let rowsCount = 0;
    for (const file of files) {
      const data = JSON.parse(fs.readFileSync(path.join(fixtures, `${file}.json`)));
      rowsCount += data.length;
    }
    const reader = new ParquetGroupReader(files.map(file => path.join(fixtures, `${file}.parquet`)));
    let readCount = 0;
    for await (const data of await reader.getIterator({shuffle: true, compressors})) {
      readCount++;
    }
    await reader.close();
    assert.deepEqual(rowsCount, readCount);
  });
});

function replaceNonJsonValues<T extends Record<string, unknown>>(obj: T) {
  for (const key in obj) {
    const value = obj[key];
    if (typeof value === 'bigint')
      obj[key] = Number(value);
    else if (value instanceof Date)
      obj[key] = value.toISOString();
    else if (value === undefined)
      obj[key] = null;
    else if (typeof value === 'number' && isNaN(value))
      obj[key] = null;
    else if (typeof value === 'object' && value !== null)
      replaceNonJsonValues(value as Record<string, unknown>);
  }
}
