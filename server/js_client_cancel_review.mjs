// SPDX-License-Identifier: GPL-3.0-only
import assert from 'node:assert/strict';
import {Client, newID} from '../client/js/index.mjs';
let input = '';
for await (const chunk of process.stdin) input += chunk;
const cfg = JSON.parse(input);
const client = await Client.connect({...cfg, format: 'cancel-review', routing: 'direct'});
const control = async (path) => {
 const response = await fetch(cfg.endpoint + '/__js_review/' + path);
 assert.equal(response.status, 200);
 return response.json();
};
try {
 if (cfg.mode === 'pending') await control('hold');
 const requestId = newID(), abort = new AbortController();
 const acquisition = client.beginObjectLease(false, {signal: abort.signal, requestId});
 // Attach a rejection handler before aborting so rejection is never unhandled.
 const rejected = assert.rejects(acquisition);
 await control((cfg.mode === 'pending' ? 'admitted' : 'dropped') + '?id=' + requestId);
 abort.abort();
 await rejected;
 if (cfg.mode === 'pending') await control('release');
 const state = await control('settled?id=' + requestId);
 assert.equal(state.maintenance, false, 'cancelled acquisition retained an exclusive lease');
 console.log('JS_CANCEL_OK ' + cfg.mode);
} finally {
 if (cfg.mode === 'pending') await control('release');
 await client.close();
}
