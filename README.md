# parquet-reader

A simple library for reading parquet files.

## Install

```sh
npm install @frost-beta/parquet-reader
```

## Import

```js
import {ParquetReader, ParquetGroupReader} from '@frost-beta/parquet-reader';
```

## API

```ts
type IteratorOptions = {
    /**
     * Shuffle the data before iterating them.
     */
    shuffle?: boolean;
    /**
     * Optional compressors.
     */
    compressors?: Compressors;
    /**
     * How many rows to read in one chunk?
     */
    chunkSize?: number;
};

export declare class ParquetReader {
    /**
     * Create a new reader for `filePath`.
     */
    constructor(filePath: string);

    /**
     * Return raw metadata of the file.
     */
    getMetadata(): Promise<FileMetaData>;

    /**
     * Return total number of rows.
     */
    getRowsCount(): Promise<number>;

    /**
     * Return an generator which can be used to iterate the data.
     */
    getIterator(options?: IteratorOptions): Promise<AsyncGenerator<unknown[]>>;

    /**
     * Close the file.
     */
    close(): Promise<void>;
}

export declare class ParquetGroupReader {
    /**
     * Create a group reader for all the `filePaths`.
     */
    constructor(filePaths: string[]);

    /**
     * Return total number of rows of all files.
     */
    getRowsCount(): Promise<number>;

    /**
     * Return an generator which can be used to iterate the data.
     */
    getIterator(options?: IteratorOptions): Promise<AsyncGenerator<unknown[]>>;

    /**
     * Close all the files.
     */
    close(): Promise<void>;
}
```

## Example

```ts
import {compressors} from 'hyparquet-compressors'
import {ParquetGroupReader} from '@frost-beta/parquet-reader';

const reader = new ParquetGroupReader([
  'path/to/train-1.parquet',
  'path/to/train-2.parquet',
  'path/to/train-3.parquet',
]);
for await (const data of await reader.getIterator({shuffle: true, compressors})) {
  console.log(data);
}
await reader.close();
```
