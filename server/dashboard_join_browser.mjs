// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const cfg=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage({viewport:{width:1280,height:900}}),errors=[];
 page.on('pageerror',e=>errors.push(e.message));
 await page.goto(cfg.url);await page.locator('#username').fill(cfg.username);await page.locator('#password').fill(cfg.password);await page.locator('#login-form button[type=submit]').click();
 await page.locator('#join-cluster').waitFor({state:'visible'});await page.locator('#join-cluster').click();
 await page.locator('#join-uri').fill(cfg.target);await page.locator('#join-username').fill(cfg.targetUser);await page.locator('#join-password').fill('incorrect');await page.locator('#join-form button[type=submit]').click();
 await page.waitForFunction(()=>document.querySelector('#join-error').textContent.length>0);
 if(!await page.locator('#join-dialog').isVisible())throw Error('Failed login closed join dialog');
 await page.locator('#join-password').fill(cfg.targetPassword);await page.locator('#join-form button[type=submit]').click();
 await page.waitForFunction(()=>!document.querySelector('#app').hidden&&document.querySelector('#topbar-node-count').textContent==='2 NODES',{},{timeout:45000});
 await page.locator('#join-dialog').waitFor({state:'hidden'});await page.locator('#join-cluster-card').waitFor({state:'hidden'});
 if(await page.locator('#join-password').inputValue()!=='')throw Error('Join password retained');
 const stored=await page.evaluate(()=>JSON.stringify({...localStorage,...sessionStorage}));if(stored.includes(cfg.targetPassword))throw Error('Join password stored');
 if(errors.length)throw Error(errors.join('\n'));
 console.log('Fresh-node join CTA, bad credentials, prepared volume, automatic target-account login and two-node dashboard passed.');
}finally{await browser.close()}
