// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
import {mkdir} from 'node:fs/promises';
import {join} from 'node:path';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const cfg=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
const output=process.env.STQR_TEST_BROWSER_OUTPUT_DIR??'/tmp';await mkdir(output,{recursive:true});
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
   return {capacity:m.dataset.capacity,used:m.dataset.used,free:m.dataset.free,width:fill.getBoundingClientRect().width,track:m.querySelector('.capacity-track').getBoundingClientRect().width,innerTrack:m.querySelector('.capacity-track').clientWidth,ticks:ticks.map(t=>t.textContent),tickPositions:ticks.map(t=>Number.parseFloat(getComputedStyle(t).left)),scaleWidth:el.querySelector('.capacity-scale').getBoundingClientRect().width,total:el.querySelector('[data-capacity-total]').textContent,occupied:el.querySelector('[data-capacity-used]').textContent,detail:el.querySelector('[data-capacity-detail]').textContent,label:m.getAttribute('aria-label'),font:Number.parseFloat(getComputedStyle(el.querySelector('[data-capacity-used]')).fontSize)};
  });
  if(result.capacity!==expected||BigInt(result.used)<=0n||BigInt(result.used)+BigInt(result.free)!==BigInt(expected))throw Error(`Invalid global totals ${JSON.stringify(result)}`);
  if(!(result.width>0&&result.width<=result.track))throw Error('Nonzero utilization has no visible scaled fill');
  const expectedWidth=result.innerTrack*Number(BigInt(result.used)*10000n/BigInt(expected))/10000;
  if(Math.abs(result.width-expectedWidth)>1)throw Error(`Utilization fill is not proportional to its byte scale: ${JSON.stringify(result)}`);
  if(result.ticks.length!==5||result.ticks[0]!=='0 B'||result.ticks[4]!==result.total||result.ticks.some(t=>t==='—'))throw Error('Absolute byte scale is missing');
  if(result.tickPositions.some((x,index)=>Math.abs(x-index*result.scaleWidth/4)>1))throw Error('Byte scale tick does not align with capacity percentage');
  if(!result.detail.includes('frei')||!result.detail.includes('nutzbar')||!result.label.includes('physisch belegt')||result.font<25)throw Error('Occupied capacity is not prominent/accessibly labeled');
  return result;
 }
 const volume=await verify('');
 await page.locator('#volume-search').fill('no-such-volume');
 if(await page.locator('#capacity-summary [data-capacity-meter]').getAttribute('data-capacity')!==expected)throw Error('Table filter changed global capacity');
 await page.locator('#volume-search').fill('');
 await page.locator('.node-toggle').first().click();
 if(await page.locator('#capacity-summary [data-capacity-meter]').getAttribute('data-capacity')!==expected)throw Error('Collapsing rows changed global capacity');
 await page.locator('.node-toggle').first().click();
 await page.screenshot({path:join(output,'capacity-volumes-desktop.png')});
 await page.locator('[data-view="buckets"]').click();const bucket=await verify('bucket-');
 if(bucket.capacity!==volume.capacity)throw Error('Bucket/volume capacity cards diverged');
 await page.screenshot({path:join(output,'capacity-buckets-desktop.png')});
 await page.setViewportSize({width:390,height:844});
 await page.screenshot({path:join(output,'capacity-buckets-mobile.png')});
 const overflow=await page.locator('#bucket-capacity-summary').evaluate(el=>{const r=el.getBoundingClientRect();return r.width>innerWidth||el.scrollWidth>el.clientWidth+1});
 if(overflow)throw Error('Global capacity card overflows mobile viewport');
 const mobileBucket=await verify('bucket-');
 await page.locator('[data-view="volumes"]').click();const mobileVolume=await verify('');
 await page.screenshot({path:join(output,'capacity-volumes-mobile.png')});
 // Controlled renderer states supplement the real two-node payload fixture;
 // they never replace its snapshots, totals or nonzero geometry assertions.
 await page.evaluate(async()=>{
  const {capacityKnown,capacityAdmissionKnown,renderCapacitySummary}=await import('/assets/capacity.js');
  const probe=document.querySelector('#capacity-summary').cloneNode(true);
  for(const el of [probe,...probe.querySelectorAll('[id]')])el.removeAttribute('id');
  Object.assign(probe.style,{position:'fixed',left:'-2000px',width:'280px'});document.body.append(probe);
  const q=name=>probe.querySelector(`[data-capacity-${name}]`),totals={capacity:100n,allocated:25n,reserved:5n,admission:70n};
  try{
   renderCapacitySummary(probe,totals,true);
   if(q('fill').getBoundingClientRect().width<=0)throw Error('Controlled known renderer has no positive fill');
   const readyColor=getComputedStyle(probe.querySelector('.capacity-track')).backgroundColor;
   renderCapacitySummary(probe,totals,true,capacityAdmissionKnown([{snapshot:{reason:1024n}}]));
   if(q('used').textContent==='—'||!q('detail').textContent.includes('wird ermittelt')||q('detail').textContent.includes('nutzbar')||!q('meter').getAttribute('aria-label').includes('noch unbekannt')||q('reserve').style.width!=='0%')throw Error('Cold reservation recovery presented admission or reserves as ready');
   if(getComputedStyle(probe.querySelector('.capacity-track')).backgroundColor===readyColor||q('available-label').textContent.includes('Nutzbar')||getComputedStyle(q('reserve-legend')).display!=='none'||q('fill').getBoundingClientRect().width<=0)throw Error('Cold capacity visually claims usable free space or hides known occupation');
   renderCapacitySummary(probe,totals,true,true);
   if(getComputedStyle(probe.querySelector('.capacity-track')).backgroundColor!==readyColor||q('available-label').textContent!=='Nutzbar'||getComputedStyle(q('reserve-legend')).display==='none')throw Error('Ready admission did not restore reserve/usable legend');
   renderCapacitySummary(probe,totals,capacityKnown([{snapshot:{volumes:[]}}],2));
   if(q('total').textContent!=='—'||q('used').textContent!=='—'||q('fill').getBoundingClientRect().width!==0||!q('meter').getAttribute('aria-label').includes('unbekannt'))throw Error('Missing member report presented partial global capacity as known');
   renderCapacitySummary(probe,{capacity:0n,allocated:0n,reserved:0n,admission:0n},capacityKnown([{snapshot:{volumes:[]}}],1));
   if(q('total').textContent!=='0 B'||q('used').textContent!=='0 B'||q('detail').textContent.includes('Warte'))throw Error('Reported empty capacity presented as unknown');
  }finally{probe.remove()}
 });
 if(errors.length)throw Error(errors.join('\n'));
 console.log(JSON.stringify({desktopVolume:volume,desktopBucket:bucket,mobileVolume,mobileBucket}));
 console.log('Both global physical summaries show prominent occupied bytes, nonzero scaled utilization, five byte ticks, free/usable labels and unchanged all-node totals across filtering; mobile fits.');
}finally{await browser.close()}
