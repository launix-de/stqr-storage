// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const config=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try {
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(config.url);await page.locator('#username').fill(config.username);await page.locator('#password').fill(config.password);await page.locator('#login-form button[type=submit]').click();
 await page.locator('[data-view="users"]').click();await page.locator(`[data-user-s3="${config.user}"]`).click();
 const dialog=page.locator('dialog:has([data-create])');await dialog.locator('[data-create]').click();await dialog.locator('[data-created]').waitFor({state:'visible'});
 const access=await dialog.locator('[data-access]').inputValue(),secret=await dialog.locator('[data-secret]').inputValue();if(!access.startsWith('STQR')||secret.length!==64)throw Error('Missing generated credentials');
 await dialog.locator('[data-keys] code').filter({hasText:access}).waitFor();await dialog.locator('button.secondary').click();
 await page.waitForFunction(()=>{const input=document.querySelector('dialog [data-secret]');return input&&!input.value&&!input.closest('dialog').open});
 await page.locator(`[data-user-s3="${config.user}"]`).click();if(await dialog.locator('[data-created]').isVisible())throw Error('Secret shown on reopen');
 const row=dialog.locator('p').filter({has:page.locator('code').filter({hasText:access})});await row.getByRole('button',{name:'Widerrufen'}).click();
 await dialog.locator('[data-keys] code').filter({hasText:access}).waitFor({state:'detached'});
 await dialog.locator('button.secondary').click();if(errors.length)throw Error(errors.join('; '));
 console.log('S3 key creation, one-time secret display, reopen and revocation verified.');
}finally{await browser.close()}
