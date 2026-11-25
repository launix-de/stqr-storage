// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const cfg=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage(),errors=[],deletes=[];page.setDefaultTimeout(15000);page.on('pageerror',e=>errors.push(e.message));
 page.on('websocket',ws=>ws.on('framesent',({payload})=>{const b=Buffer.from(payload);if(b.length>=37&&[0x200e,0x2008].includes(b.readUInt16LE(2)))deletes.push({op:b.readUInt16LE(2),id:b.subarray(4,20).toString('hex'),target:b.subarray(20,36).toString('hex'),option:b[36]})}));
 await page.goto(cfg.url);await page.evaluate(()=>{window.deletionToasts=[];new MutationObserver(records=>{for(const r of records)for(const n of r.addedNodes)if(n.nodeType===1)window.deletionToasts.push({text:n.textContent,error:n.classList.contains('error-toast')})}).observe(document.querySelector('#toasts'),{childList:true})});await page.locator('#username').fill(cfg.username);await page.locator('#password').fill(cfg.password);await page.locator('#login-form button[type=submit]').click();await page.locator('[data-view="users"]').click();
 for(const name of ['first','second']){
  await page.locator(`[data-user-delete="${cfg.users[name]}"]`).click();
  const option=page.locator('#user-delete-buckets');if(!await option.isChecked())throw Error('user deletion default lost');if(name==='second')await option.uncheck();
  await page.locator('#user-delete-form button[type=submit]').click();await page.locator('#user-delete-dialog').waitFor({state:'hidden'});
  await page.locator(`[data-user="${cfg.users[name]}"]`).filter({hasText:'Löschung ausstehend'}).waitFor();
 }
 await page.waitForFunction(async()=>await(await fetch('/_test/delete-entered')).json());
 // A second confirmed deletion is queued locally; it must not occupy another
 // RPC slot or prevent refresh/navigation while the first commit is gated.
 if(deletes.length!==1)throw Error('destructive requests were not bounded by the shared queue');
 await page.locator('#refresh-users').click();await page.waitForFunction(()=>!document.querySelector('#refresh-users').disabled);
 for(const name of ['first','second'])await page.locator(`[data-user="${cfg.users[name]}"]`).filter({hasText:'Löschung ausstehend'}).waitFor();
 await page.locator('[data-view="buckets"]').click();
 const openDelete=async name=>{await page.locator(`[data-bucket-action="delete"][data-id="${cfg.buckets[name]}"]`).click();if(!await page.locator('#bucket-delete-users-option').isVisible()||!await page.locator('#bucket-delete-users').isChecked())throw Error('bucket delete option missing/default off')};
 await openDelete('drop');await page.locator('#bucket-action-submit').click();await page.locator('#bucket-action-dialog').waitFor({state:'hidden'});
 await openDelete('keep');await page.locator('#bucket-delete-users').uncheck();await page.locator('#bucket-action-submit').click();await page.locator('#bucket-action-dialog').waitFor({state:'hidden'});
 // The earlier bucket deletion will remove this account before its own
 // queued user deletion arrives. Its 404 confirms absence, not a failure.
 await page.locator('[data-view="users"]').click();await page.locator(`[data-user-delete="${cfg.users.orphan}"]`).click();await page.locator('#user-delete-form button[type=submit]').click();await page.locator('#user-delete-dialog').waitFor({state:'hidden'});
 await page.evaluate(()=>fetch('/_test/release-delete',{method:'POST'}));
 await page.locator('[data-view="users"]').click();
 if(cfg.fails){
  await page.locator(`[data-user="${cfg.users.first}"] .error`).waitFor();await page.locator(`[data-user-delete="${cfg.users.first}"]`).click();
  if(!await page.locator('#user-delete-buckets').isChecked())throw Error('retry changed user deletion choice');
  await page.locator('#user-delete-form button[type=submit]').click();await page.locator('#user-delete-dialog').waitFor({state:'hidden'});
 }
 for(const name of ['first','second','orphan'])await page.locator(`[data-user="${cfg.users[name]}"]`).waitFor({state:'hidden'});
 await page.waitForFunction(()=>window.deletionToasts.some(t=>t.text==='User „orphan“ wurde gelöscht.'));if(await page.evaluate(()=>window.deletionToasts.some(t=>t.error&&t.text.includes('orphan'))))throw Error('already absent account reported as failed deletion');
 for(const name of ['shared','administrator','kept-owner'])await page.locator(`[data-user="${cfg.users[name]}"]`).waitFor();
 await page.locator('[data-view="buckets"]').click();
 await page.waitForFunction(()=>!document.querySelector('#bucket-rows').textContent.includes('Löschung ausstehend'));
 await page.locator(`[data-bucket-action="rename"][data-id="${cfg.buckets.other}"]`).click();if(await page.locator('#bucket-delete-users-option').isVisible())throw Error('delete option leaked into rename dialog');
 const first=deletes.filter(d=>d.target===cfg.users.first);if(cfg.fails&&(first.length!==2||first[0].id!==first[1].id))throw Error('retry did not preserve operation ID');
 for(const [name,want]of [['drop',1],['keep',0]]){const d=deletes.find(d=>d.target===cfg.buckets[name]);if(!d||d.option!==want)throw Error('bucket deletion option not encoded')}
 if(errors.length)throw Error(errors.join('\n'));
 console.log('Delayed deletion keeps navigation and refresh responsive; consecutive deletions, checkbox defaults/opt-out, protected accounts and stable retry IDs passed.');
}finally{await browser.close()}
