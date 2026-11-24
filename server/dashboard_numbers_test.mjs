// SPDX-License-Identifier: GPL-3.0-only
import test from 'node:test';
import assert from 'node:assert/strict';
import {formatNumber,formatBytes,formatRate} from './dashboard/numbers.js';
test('display quantities share German grouping and at most one fractional digit',()=>{
 assert.equal(formatNumber(1234567.893),'1.234.567,9');
 assert.equal(formatNumber(1234567890123456789n),'1.234.567.890.123.456.789');
 assert.equal(formatRate(179983n),'175,8 KiB/s');
 assert.equal(formatBytes(6n*1024n**2n),'6 MiB');
 assert.equal(formatBytes(1024n**2n-1n),'1 MiB');
 assert.equal(formatBytes(0n),'0 B');
 assert.equal(formatRate(null),'—');assert.equal(formatNumber(NaN),'—');
});
