// SPDX-License-Identifier: GPL-3.0-only
// Invoked only by the opt-in Go browser integration test. Credentials arrive on
// stdin and must never appear in command arguments, screenshots or output.
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url);
const {chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const config=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(config.url);
 await page.locator('#username').fill(config.username);await page.locator('#password').fill(config.password);
 await page.locator('#login-form button[type=submit]').click();
 await page.waitForFunction(()=>document.querySelectorAll('.node-row').length===2);
 const row=page.locator(`tr.node-row[data-node="${config.target}"]`);
 if(await row.locator('.row-actions button').count()!==1)throw Error('Node row must offer only the maintenance button.');
 for(const [kind,dialog]of [['short','node-action-dialog'],['drain','node-action-dialog'],['replace','recovery-node-dialog'],['force','force-dialog']]){
  await page.locator('tr.node-row').first().locator('[data-node-maintenance]').click();
  if(await page.locator('#node-maintenance-dialog .maintenance-option').count()!==4)throw Error('Expected four explained maintenance actions.');
  if(!await page.locator('#maintenance-'+kind).isEnabled()){
   if(!await page.locator('#maintenance-'+kind+'-note').isVisible())throw Error('Disabled maintenance action needs an explanation.');
   await page.locator('[data-close="node-maintenance-dialog"]').first().click();continue;
  }
  await page.locator('#maintenance-'+kind).click();
  if(!await page.locator('#'+dialog).evaluate(e=>e.open))throw Error('Maintenance action did not open its confirmation: '+kind);
  await page.locator(`#${dialog} [data-close="${dialog}"]`).first().click();
 }
 const add=page.locator(`[data-add-node="${config.target}"]`);await add.waitFor();
 await add.click();await page.locator('#volume-name').fill('browser-remote');
 await page.locator('#volume-size').fill('1M');await page.locator('#add-submit').click();
 await page.waitForFunction(()=>!document.querySelector('#add-dialog').open);
 const remote=page.locator(`tr[data-volume][data-node="${config.target}"]`).filter({hasText:'browser-remote'});
 await remote.waitFor();
 await remote.locator('[data-grow]').click();await page.locator('#grow-size').fill('2M');await page.locator('#grow-submit').click();
 await page.waitForFunction(()=>!document.querySelector('#grow-dialog').open);
 await page.waitForFunction(target=>[...document.querySelectorAll(`tr[data-volume][data-node="${target}"]`)].some(row=>row.textContent.includes('browser-remote')&&row.textContent.includes('2 MiB')),config.target);
 if(errors.length)throw Error(errors.join('; '));
 if(process.env.STQR_TEST_BROWSER_SCREENSHOT)await page.screenshot({path:process.env.STQR_TEST_BROWSER_SCREENSHOT,fullPage:true});
 console.log('Two node rows; remote volume creation and growth via native WebSocket messages verified.');
}finally{await browser.close()}
