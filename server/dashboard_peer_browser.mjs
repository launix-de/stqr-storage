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
