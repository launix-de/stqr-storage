// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const config=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE,headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage(),errors=[];page.setDefaultTimeout(10000);
 page.on('pageerror',e=>errors.push(e.message));
 await page.goto(config.url);console.log("Login page loaded");
 await page.locator('#username').fill(config.username);await page.locator('#password').fill(config.password);await page.locator('#login-form button[type=submit]').click();
 await page.locator('[data-view="users"]').click();console.log('User view loaded');
 await page.locator(`[data-user-delete="${config.keep}"]`).click();
 console.log('First dialog opened');const option=page.getByLabel('Buckets löschen, die nur diesem User gehören');
 if(!await option.isChecked())throw Error('Deletion option is not checked by default');
 await option.uncheck();await page.locator('#user-delete-form button[type=submit]').click();await page.locator('#user-delete-dialog').waitFor({state:'hidden'});
 console.log('First deletion completed');await page.locator(`[data-user-delete="${config.remove}"]`).click();
 if(!await option.isChecked())throw Error('Reopening the dialog did not reset the default');
 await page.locator('#user-delete-form button[type=submit]').click();await page.locator('#user-delete-dialog').waitFor({state:'hidden'});
 await page.locator('[data-view="buckets"]').click();
 for(const name of ['keep-data','shared-data'])await page.locator('#bucket-rows tr').filter({hasText:name}).waitFor();
 if(errors.length)throw Error(errors.join('\n'));
 console.log('User deletion defaults, opt-out, shared buckets and refreshed bucket view passed.');
}finally{await browser.close()}
