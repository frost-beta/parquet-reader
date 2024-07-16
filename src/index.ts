import fs from 'node:fs/promises';
import {
  parquetMetadataAsync,
  parquetRead,
  AsyncBuffer,
  Compressors,
  FileMetaData,
} from 'hyparquet';

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

export class ParquetReader {
  filePath: string;
  private handle?: fs.FileHandle;
  private buffer?: AsyncBuffer;
  private metadata?: FileMetaData;

  /**
   * Create a new reader for `filePath`.
   */
  constructor(filePath: string) {
    this.filePath = filePath;
  }

  /**
   * Return raw metadata of the file.
   */
  async getMetadata(): Promise<FileMetaData> {
    if (!this.metadata)
      this.metadata = await parquetMetadataAsync(await this.getAsyncBuffer());
    return this.metadata;
  }

  /**
   * Return total number of rows.
   */
  async getRowsCount(): Promise<number> {
    return Number((await this.getMetadata()).num_rows);
  }

  /**
   * Return an generator which can be used to iterate the data.
   */
  async getIterator(options: IteratorOptions = {}): Promise<AsyncGenerator<unknown[]>> {
    // Read rows in chunks for much better performance.
    const chunkSize = options.chunkSize ?? 128;
    // For n rows, indices is [0, 16, 16 * 2, ..., 16 * M, 16 * M + X].
    const rowsCount = await this.getRowsCount();
    const indices = new Array(Math.ceil(rowsCount / chunkSize))
    for (let i = 0; i < indices.length; ++i) {
      if (i > Math.floor(rowsCount / chunkSize))
        indices[i] = (i - 1) * chunkSize;
      else
        indices[i] = i * chunkSize;
    }
    if (options.shuffle)
      shuffle(indices);
    // Read the parquet file.
    const file = await this.getAsyncBuffer();
    const metadata = await this.getMetadata();
    return (async function*() {
      for (const index of indices) {
        let rows: unknown[][] = [];
        await parquetRead({
          file,
          metadata,
          compressors: options.compressors,
          rowStart: index,
          rowEnd: Math.min(index + chunkSize, rowsCount),
          onComplete: (data) => rows = data,
        });
        for (const row of rows!)
          yield row;
      }
    })();
  }

  /**
   * Close the file.
   */
  async close() {
    if (this.handle) {
      await this.handle.close();
      this.handle = undefined;
      this.metadata = undefined;
    }
  }

  private async getAsyncBuffer() {
    if (!this.handle)
      this.handle = await fs.open(this.filePath);
    if (!this.buffer)
      this.buffer = await handleToAsyncBuffer(this.handle);
    return this.buffer;
  }
};

export class ParquetGroupReader {
  readers: ParquetReader[] = [];
  private rowsCount?: number;

  /**
   * Create a group reader for all the `filePaths`.
   */
  constructor(filePaths: string[]) {
    this.readers = filePaths.map(filePath => new ParquetReader(filePath));
  }

  /**
   * Return total number of rows of all files.
   */
  async getRowsCount(): Promise<number> {
    if (this.rowsCount === undefined) {
      const counts = await Promise.all(this.readers.map(reader => reader.getRowsCount()));
      this.rowsCount = counts.reduce((a, b) => a + b, 0);
    }
    return this.rowsCount;
  }

  /**
   * Return an generator which can be used to iterate the data.
   */
  async getIterator(options: IteratorOptions = {}): Promise<AsyncGenerator<unknown[]>> {
    const iterators = await Promise.all(this.readers.map(reader => reader.getIterator(options)));
    return (async function*() {
      if (options.shuffle) {
        while (iterators.length > 0) {
          const index = Math.floor(Math.random() * iterators.length);
          const {done, value} = await iterators[index].next();
          if (done)
            iterators.splice(index, 1);
          else
            yield value;
        }
      } else {
        for (const iterator of iterators) {
          for await (const data of iterator)
            yield data;
        }
      }
    })();
  }

  /**
   * Close all the files.
   */
  async close() {
    await Promise.all(this.readers.map(reader => reader.close()));
  }
};

async function handleToAsyncBuffer(handle: fs.FileHandle): Promise<AsyncBuffer> {
  const stats = await handle.stat();
  return {
    byteLength: stats.size,
    slice: async (position: number, end?: number) => {
      let offset = 0;
      let length = (end ?? stats.size) - position;
      const buffer = Buffer.alloc(length);
      while (length > 0) {
        // The fileHanlde.read API has a hard limit for one read.
        const chunkSize = Math.min(length, 16384);
        const {bytesRead} = await handle.read(buffer, offset, chunkSize, position);
        position += bytesRead;
        offset += bytesRead;
        length -= bytesRead;
      }
      return buffer.buffer;
    },
  };
};

function shuffle(array: unknown[]) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
}
