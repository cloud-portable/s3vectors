import type { DataSpec } from './index.js'

export type DerivedField =
  | 'size' | 'md5' | 'etag' | 'sha256' | 'sha256B64' | 'sha1B64'
  | 'crc32B64' | 'crc32cB64' | 'crc64nvmeB64'

/** The fields available as `${data.<name>.<field>}` placeholders. */
export const DERIVED_FIELDS: readonly DerivedField[]

/** Bytes per chunk when streaming or digesting. */
export const CHUNK_SIZE: number

/** Materialize one named dataset from a vector's `data` map. */
export function generate (specs: Record<string, DataSpec>, name: string): Buffer

/** Materialize `[offset, offset+length)` of a dataset without materializing the rest. */
export function generateRange (
  specs: Record<string, DataSpec>, name: string, offset: number, length: number
): Buffer

export interface GenerateStreamOptions {
  /** First byte of the dataset to emit. Default 0. */
  offset?: number
  /** Bytes to emit. Default: to the end of the dataset. */
  length?: number
  /** Bytes per chunk; the last chunk may be shorter. Default CHUNK_SIZE. */
  chunkSize?: number
}

/**
 * A bounded-memory byte stream over a dataset (or a range of one) — the only way
 * to read a dataset larger than the platform's maximum allocation.
 */
export function generateStream (
  specs: Record<string, DataSpec>, name: string, opts?: GenerateStreamOptions
): ReadableStream<Buffer>

/** The dataset's declared length in bytes, without generating it. */
export function dataSize (specs: Record<string, DataSpec>, name: string): number

/** Compute the string a `${data.<name>.<field>}` placeholder resolves to. */
export function derived (specs: Record<string, DataSpec>, name: string, field: DerivedField): string
