// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const config=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(config.url);await page.locator('#username').fill(config.username);await page.locator('#password').fill(config.password);await page.locator('#login-form button[type=submit]').click();
 await page.locator('[data-view="groups"]').click();await page.locator('#pg-add').click();await page.locator('#pg-name').fill('Hosting DB');
 await page.locator('#pg-node-selection').selectOption('list');await page.locator('#pg-add-node').click();
 const mode=page.locator('[data-mode]').first();await mode.selectOption('blacklist');if(!await page.locator('[data-volume-id]').isVisible())throw Error('Blacklist volume editor hidden');
 await mode.selectOption('all');if(await page.locator('[data-volume-id]').isVisible())throw Error('All still displays volume list');
 await page.locator('#pg-node-selection').selectOption('all');await page.locator('#pg-submit').click();await page.locator('#pg-dialog').waitFor({state:'hidden'});
 for(const name of ['customer-a-db','customer-b-db']){
  await page.locator('[data-view="buckets"]').click();await page.locator('#add-bucket').click();await page.locator('#bucket-name').fill(name);await page.locator('#bucket-pg').selectOption({label:'Hosting DB'});
  if(!await page.locator('#bucket-compression').isDisabled())throw Error('Bucket allows codec override');
  await page.locator('#bucket-create-submit').click();await page.locator('#bucket-create-dialog').waitFor({state:'hidden'});
  await page.locator('#bucket-rows tr').filter({hasText:name}).waitFor();
 }
 await page.locator('[data-view="groups"]').click();const row=page.locator('#pg-rows tr').filter({hasText:'Hosting DB'});await row.locator('[data-edit-pg]').click();await page.locator('#pg-codec').selectOption('gzip');await page.locator('#pg-submit').click();await page.locator('#pg-dialog').waitFor({state:'hidden'});
 await page.waitForFunction(()=>[...document.querySelectorAll('#pg-rows tr')].some(r=>r.textContent.includes('Hosting DB')&&r.textContent.includes('Aktiv · v2')));
 await page.locator('[data-view="buckets"]').click();for(const name of ['customer-a-db','customer-b-db'])await page.locator('#bucket-rows tr').filter({hasText:name}).filter({hasText:'GZIP'}).waitFor();
 await page.locator('[data-view="groups"]').click();await page.locator('#pg-add').click();await page.locator('#pg-name').fill('Archive exclusions');await page.locator('#pg-node-selection').selectOption('list');await page.locator('#pg-add-node').click();await page.locator('[data-mode]').selectOption('blacklist');await page.locator('#pg-submit').click();await page.locator('#pg-dialog').waitFor({state:'hidden'});
 await page.locator('[data-view="volumes"]').click();await page.locator('[data-add-node]').first().click();await page.locator('#volume-name').fill('pg-browser-volume');await page.locator('#volume-size').fill('2M');await page.locator('#volume-pg-selection label').filter({hasText:'Archive exclusions'}).locator('input').check();await page.locator('#add-submit').click();await page.locator('#add-dialog').waitFor({state:'hidden'});
 await page.locator('[data-view="groups"]').click();await page.locator('#pg-rows tr').filter({hasText:'Archive exclusions'}).locator('[data-edit-pg]').click();await page.locator('#pg-nodes').getByText('pg-browser-volume',{exact:true}).waitFor();await page.locator('#pg-cancel').click();
 if(errors.length)throw Error(errors.join('\n'));console.log('Volume addition updates selected blacklist and releases its placement hold.');console.log('PG editor, volume filters, shared bucket settings and central codec update passed.');
}finally{await browser.close()}
