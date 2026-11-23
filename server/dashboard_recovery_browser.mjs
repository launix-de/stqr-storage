// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const config=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(config.url);await page.locator('#username').fill(config.username);await page.locator('#password').fill(config.password);
 await page.locator('#login-form button[type=submit]').click();
 await page.locator(`[data-node-replace="${config.source}"]`).click();
 await page.locator('#recovery-node-submit').click();
 if(!await page.locator('#recovery-node-dialog').evaluate(e=>e.open))throw Error('Fencing confirmation must be required.');
 await page.locator('#recovery-node-fenced').check();await page.locator('#recovery-node-submit').click();
 await page.waitForFunction(()=>!document.querySelector('#recovery-node-dialog').open);
 await page.locator(`[data-recovery-volumes="${config.target}"]`).click();
 await page.locator('#recovery-volume-name').fill('saved-browser');await page.locator('#recovery-volume-path').fill(config.path);
 await page.locator('#recovery-stage-form button[type=submit]').click();
 await page.locator(`#recovery-volume-list input[value="${config.volume}"]`).waitFor();
 await page.locator('#recovery-volume-list').getByRole('button',{name:'Freigeben',exact:true}).click();
 await page.locator(`#recovery-volume-list input[value="${config.volume}"]`).waitFor({state:'detached'});
 await page.locator('#recovery-volume-name').fill('saved-browser');await page.locator('#recovery-volume-path').fill(config.path);
 await page.locator('#recovery-stage-form button[type=submit]').click();
 await page.locator(`#recovery-volume-list input[value="${config.volume}"]`).check();
 await page.locator('#recovery-volume-submit').click();
 await page.waitForFunction(()=>!document.querySelector('#recovery-volume-dialog').open);
 await page.locator(`tr[data-volume="${config.volume}"]`).filter({hasText:'saved-browser'}).waitFor();
 if(errors.length)throw Error(errors.join('; '));
 console.log('Native recovery begin, fencing confirmation, staged media, release/reopen and committed activation verified in browser.');
}finally{await browser.close()}
