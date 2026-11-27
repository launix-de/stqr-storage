// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;const cfg=JSON.parse(input);
const browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try {
 const page=await browser.newPage();const sockets=[],errors=[];page.on('websocket',ws=>sockets.push(ws.url()));page.on('pageerror',e=>errors.push(e.message));
 await page.goto(cfg.url);await page.locator('#username').fill(cfg.username);await page.locator('#password').fill(cfg.password);await page.locator('#login-form button[type=submit]').click();await page.locator('#app').waitFor({state:'visible'});
 for(const routing of ['gateway','direct']){
  sockets.length=0;
  await page.evaluate(async({bucket,routing,username,password})=>{
   const {Client,sha256,hex,newID}=await import('/assets/sdk/index.mjs'),data=new TextEncoder().encode('browser-native-'+routing),id=hex(sha256(data));const session=await(await fetch('/api/session')).json();
   const c=await Client.connect({endpoint:location.origin,bucket,format:'js-browser',routing,csrf:session.csrf_token,...(routing==='direct'?{credentials:async()=>({username,password})}:{})});
   try{await c.put(id,data,{accepted:true});await c.sync();const read=await c.get(id);if(read.data.some((v,i)=>v!==data[i])||read.data.length!==data.length)throw Error('Browser native data mismatch');const token=newID();await c.pinObject(id,token);await c.unpinObject(id,token);await c.delete(id);if(routing==='gateway')window.tunnelClient=c;else await c.close()}catch(e){await c.close();throw e}
  },{bucket:cfg.bucket,routing,username:cfg.username,password:cfg.password});
  if(routing==='gateway'){
   if(!sockets.some(url=>url.includes('/api/bucket-tunnel?node=')))throw Error('Gateway did not exercise a remote tunnel');
   if(sockets.some(url=>new URL(url).host!==new URL(cfg.url).host))throw Error('Gateway browser connected directly to an owner');
  }else if(!sockets.some(url=>new URL(url).host===new URL(cfg.target).host))throw Error('Direct mode never reached owner');
 }
 await page.evaluate(async()=>{const session=await(await fetch('/api/session')).json();await fetch('/api/session',{method:'DELETE',headers:{'X-CSRF-Token':session.csrf_token}})});
 await page.waitForFunction(()=>[...window.tunnelClient.transports.values()].filter(t=>t.ws.url.includes('/api/bucket-tunnel')).every(t=>t.closed),{},{timeout:7000});await page.evaluate(()=>window.tunnelClient.close());
 if(errors.length)throw Error(errors.join('\n'));
 console.log('JS_BROWSER_OK direct, cookie gateway, owner routing and logout revocation');
}finally{await browser.close()}
