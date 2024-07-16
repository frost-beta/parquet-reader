import {ParquetGroupReader} from '../src/index';

const files = process.argv.slice(2);

const reader = new ParquetGroupReader(files);

const iterator = await reader.getIterator({shuffle: true, chunkSize: 512});
console.time('benchmark');
let count = 0;
for await (const data of iterator) {
  if (++count > 10000)
    break;
}
console.timeEnd('benchmark');
