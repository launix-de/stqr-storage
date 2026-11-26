// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const cfg=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage({viewport:{width:1500,height:1100}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(cfg.url);await page.locator('#username').fill(cfg.username);await page.locator('#password').fill(cfg.password);await page.locator('#login-form button[type=submit]').click();
 const expected=cfg.capacity;
 // Playwright 1.50 implements waitForFunction with in-page string eval,
 // which strict CSP correctly rejects. Poll direct locator functions instead;
 // keep the real dashboard policy enabled throughout the browser test.
 const meter=page.locator('#capacity-summary [data-capacity-meter]');let ready=false;
 for(const deadline=Date.now()+30000;Date.now()<deadline;){
  ready=await meter.evaluate((m,{total,minimum})=>m.dataset.capacity===total&&BigInt(m.dataset.used??'0')>=BigInt(minimum),{total:expected,minimum:cfg.minimumUsed});
  if(ready)break;await new Promise(resolve=>setTimeout(resolve,100));
 }
 if(!ready)throw Error('Global capacity did not receive all node reports and acknowledged payload');
 async function verify(prefix){
  const card=page.locator(`#${prefix}capacity-summary`);await card.waitFor({state:'visible'});
  const result=await card.evaluate(el=>{
   const m=el.querySelector('[data-capacity-meter]'),fill=el.querySelector('[data-capacity-fill]'),ticks=[...el.querySelectorAll('[data-capacity-tick]')];
   return {capacity:m.dataset.capacity,used:m.dataset.used,free:m.dataset.free,width:fill.getBoundingClientRect().width,track:m.querySelector('.capacity-track').getBoundingClientRect().width,ticks:ticks.map(t=>t.textContent),tickPositions:ticks.map(t=>Number.parseFloat(getComputedStyle(t).left)),scaleWidth:el.querySelector('.capacity-scale').getBoundingClientRect().width,total:el.querySelector('[data-capacity-total]').textContent,occupied:el.querySelector('[data-capacity-used]').textContent,detail:el.querySelector('[data-capacity-detail]').textContent,label:m.getAttribute('aria-label'),font:Number.parseFloat(getComputedStyle(el.querySelector('[data-capacity-used]')).fontSize)};
  });
  if(result.capacity!==expected||BigInt(result.used)<=0n||BigInt(result.used)+BigInt(result.free)!==BigInt(expected))throw Error(`Invalid global totals ${JSON.stringify(result)}`);
  if(!(result.width>0&&result.width<=result.track))throw Error('Nonzero utilization has no visible scaled fill');
  if(result.ticks.length!==5||result.ticks[0]!=='0 B'||result.ticks[4]!==result.total||result.ticks.some(t=>t==='—'))throw Error('Absolute byte scale is missing');
  if(result.tickPositions.some((x,index)=>Math.abs(x-index*result.scaleWidth/4)>1))throw Error('Byte scale tick does not align with capacity percentage');
  if(!result.detail.includes('frei')||!result.detail.includes('nutzbar')||!result.label.includes('physisch belegt')||result.font<25)throw Error('Occupied capacity is not prominent/accessibly labeled');
  return result;
 }
 const volume=await verify('');
 await page.locator('#volume-search').fill('no-such-volume');
 if(await page.locator('#capacity-summary [data-capacity-meter]').getAttribute('data-capacity')!==expected)throw Error('Table filter changed global capacity');
 await page.locator('#volume-search').fill('');
 await page.screenshot({path:'/tmp/stqr-global-capacity-volumes.png'});
 await page.locator('[data-view="buckets"]').click();const bucket=await verify('bucket-');
 if(bucket.capacity!==volume.capacity)throw Error('Bucket/volume capacity cards diverged');
 await page.screenshot({path:'/tmp/stqr-global-capacity-buckets.png'});
 await page.setViewportSize({width:390,height:844});
 const overflow=await page.locator('#bucket-capacity-summary').evaluate(el=>{const r=el.getBoundingClientRect();return r.width>innerWidth||el.scrollWidth>el.clientWidth+1});
 if(overflow)throw Error('Global capacity card overflows mobile viewport');
 if(errors.length)throw Error(errors.join('\n'));
 console.log('Both global physical summaries show prominent occupied bytes, nonzero scaled utilization, five byte ticks, free/usable labels and unchanged all-node totals across filtering; mobile fits.');
}finally{await browser.close()}
