// SPDX-License-Identifier: GPL-3.0-only
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const config=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(config.url);await page.locator('#username').fill(config.username);await page.locator('#password').fill(config.password);await page.locator('#login-form button[type=submit]').click();
 await page.locator('[data-view="groups"]').click();await page.locator('#pg-add').click();await page.locator('#pg-name').fill('Replicated DB');
 const copies=page.locator('#pg-replicas'),placement=page.locator('#pg-placement');
 assert.equal(await copies.isEditable(),true);await copies.fill('2');
 async function save(){await page.locator('#pg-submit').click();await page.locator('#pg-dialog').waitFor({state:'hidden'})}
 async function edit(){await page.locator('#pg-rows tr').filter({hasText:'Replicated DB'}).locator('[data-edit-pg]').click()}
 await save();await edit();assert.equal(await copies.inputValue(),'2');
 // Changing another setting must retain the saved replica count.
 await page.locator('#pg-codec').selectOption('gzip');await save();await edit();assert.equal(await copies.inputValue(),'2');
 assert.equal(await page.locator('#pg-codec').inputValue(),'gzip');
 // EVERYWHERE has a zero-count wire contract, not a disabled count of two.
 await placement.selectOption('everywhere');assert.equal(await copies.isDisabled(),true);await save();await edit();
 assert.equal(await placement.inputValue(),'everywhere');assert.equal(await copies.isDisabled(),true);
 await placement.selectOption('count');assert.equal(await copies.isEditable(),true);await copies.fill('1');await save();await edit();
 assert.equal(await copies.inputValue(),'1');
 // The unsupported RAM path remains visible and cannot save multiple copies.
 await page.locator('#pg-storage').selectOption('ram');assert.equal(await copies.getAttribute('max'),'1');
 assert.match(await page.locator('#pg-replication-note').textContent(),/RAM.*nur eine Kopie/);
 await copies.fill('2');await page.locator('#pg-submit').click();assert.equal(await page.locator('#pg-dialog').isVisible(),true);
 await placement.selectOption('everywhere');await page.locator('#pg-submit').click();await page.locator('#pg-edit-error').filter({hasText:'RAM unterstützt derzeit nur eine Kopie'}).waitFor();
 // Recover from that validation error and create an actual replicated bucket.
 await page.locator('#pg-storage').selectOption('hdd');await placement.selectOption('count');await copies.fill('2');await save();await edit();assert.equal(await copies.inputValue(),'2');await page.locator('#pg-cancel').click();
 await page.locator('[data-view="buckets"]').click();await page.locator('#add-bucket').click();await page.locator('#bucket-name').fill('browser-replicated-db');await page.locator('#bucket-pg').selectOption({label:'Replicated DB'});
 await page.locator('#bucket-create-submit').click();await page.locator('#bucket-create-dialog').waitFor({state:'hidden'});await page.locator('#bucket-rows tr').filter({hasText:'browser-replicated-db'}).waitFor();
 assert.deepEqual(errors,[]);console.log('Replica count, EVERYWHERE, single-copy roundtrip, RAM validation and replicated bucket creation passed.');
}finally{await browser.close()}
