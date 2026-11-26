// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const config=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage({bypassCSP:true,viewport:{width:1700,height:1050}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(config.url);await page.locator('#username').fill(config.username);await page.locator('#password').fill(config.password);await page.locator('#login-form button[type=submit]').click();
 // Both volume storage-access rates must be nonzero; mmap reads are included
 // once alongside positioned reads, without claiming physical-device traffic.
 await page.waitForFunction(id=>{const cell=document.querySelector(`[data-volume="${id}"] .io-rates`),rates=cell?.querySelectorAll('strong');return rates?.length===2&&[...rates].every(r=>!r.textContent.includes('—')&&r.textContent!=='0 B/s')},config.volume);
 const tooltip=await page.locator(`[data-volume="${config.volume}"] .io-rates`).getAttribute('title');
 if(!tooltip?.includes('RAM/mmap')||!tooltip.includes('kein Datenträger-I/O'))throw Error('Volume access metric is mislabeled as physical disk I/O');
 await page.screenshot({path:'/tmp/stqr-throughput-volumes.png'});
 await page.locator('[data-view="buckets"]').click();
 await page.waitForFunction(id=>{const cell=document.querySelector(`[data-bucket-throughput="${id}"]`);return cell&&!cell.textContent.includes('—')&&!cell.textContent.includes(' 0 B/s')},config.bucket);
 await page.screenshot({path:'/tmp/stqr-throughput-buckets.png'});
 const lateID=await page.evaluate(async source=>{
  const session=await(await fetch('/api/session')).json();const {NodeConnection,newID}=await import('/assets/protocol.js');
  return new Promise((resolve,reject)=>{const c=new NodeConnection(session.csrf_token,()=>{},()=>{});c.onOpen=async()=>{try{const original=(await c.listBuckets()).buckets.find(b=>b.id===source);const id=newID();await c.createBucket({...original,id,name:'late-created-bucket',grants:[]});resolve(id)}catch(e){reject(e)}finally{c.stop()}}});
 },config.bucket);
 await page.locator(`[data-bucket="${lateID}"]`).waitFor({timeout:12000});

 await page.locator('[data-view="users"]').click();
 await page.waitForFunction(()=>document.querySelectorAll('#user-rows [data-user]').length===4);
 await page.waitForFunction(()=>{const rows=[...document.querySelectorAll('#user-rows [data-user-throughput]')];return rows.length===4&&rows.every(r=>!r.textContent.includes('—'))});
 const admin=page.locator(`[data-user="${config.user}"]`);
 if((await admin.locator('[data-user-throughput]').innerText()).includes('Schreiben 0 B/s'))throw Error('Active user throughput missing');
 await page.screenshot({path:'/tmp/stqr-throughput-users.png'});
 if(errors.length)throw Error(errors.join('\n'));
 console.log('Nonzero volume storage-access read/write and logical bucket/user read/write rates render correctly; labels distinguish RAM/mmap from disk I/O.');
}finally{await browser.close()}
