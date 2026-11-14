import { describe, expect, test } from 'bun:test'
import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join } from 'node:path'

const ROOT = join(import.meta.dir, '..', 'src', 'review')
const DOTS = /[\u00B7\u2219\u22C5]/

function files(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name)
    return statSync(path).isDirectory() ? files(path) : [path]
  })
}

describe('review UI text', () => {
  test('contains no middle dots', () => {
    const offenders = files(ROOT).flatMap((path) =>
      readFileSync(path, 'utf8')
        .split('\n')
        .flatMap((line, index) => (DOTS.test(line) ? [`${path}:${index + 1}`] : [])),
    )
    expect(offenders).toEqual([])
  })
})
