// SPDX-License-Identifier: GPL-3.0-only
// Format-neutral cursor list. The provider owns query semantics and item order.
export class EndlessList {
 constructor({container,fetchPage,entries=p=>p.items,key=e=>e.id,compare=null,onUpdate,onError=()=>{},onLoading=()=>{},visible=()=>true,size=50,root=null}){
  Object.assign(this,{fetchPage,entries,key,compare,onUpdate,onError,onLoading,visible,size,root});this.items=[];this.cursor='';this.more=false;this.epoch=0;this.busy=false;this.meta={};this.cursors=new Set;this.destroyed=false;
  this.sentinel=document.createElement('div');this.sentinel.className='endless-status';this.sentinel.setAttribute('role','status');container.append(this.sentinel);
  this.observer=new IntersectionObserver(entries=>{if(entries.some(e=>e.isIntersecting))this.loadMore()},{root,rootMargin:'400px'});this.observer.observe(this.sentinel);
 }
 async reset(query=''){
  if(this.destroyed)return;
  this.cancel();this.query=query;this.items=[];this.cursor='';this.cursors.clear();this.more=true;this.error=false;this.meta={};this.onUpdate(this.items,{meta:this.meta,append:false,loading:true});return this.loadMore(true);
 }
 cancel(){this.epoch++;this.controller?.abort();this.more=false;this.busy=false;this.onLoading(false)}
 async loadMore(initial=false){
  if(this.destroyed||this.busy||!this.more||!initial&&!this.visible())return;
  const epoch=this.epoch,after=this.cursor,controller=this.controller=new AbortController;this.busy=true;this.error=false;this.onLoading(true);this.status();
  try{
   const page=await this.fetchPage(after,this.size,this.query,{signal:controller.signal});if(epoch!==this.epoch||this.destroyed)return;
   const next=page.next??'';if(next&&(next===after||this.cursors.has(next)))throw Error('Ungültiger Folgecursor.');
   const incoming=this.entries(page),items=[...this.items],indices=new Map(items.map((entry,i)=>[this.key(entry),i]));let overlap=false;const added=[];
   for(const entry of incoming){const key=this.key(entry);if(key==null)throw Error('Eintrag ohne Listenschlüssel.');const i=indices.get(key);if(i!==undefined){items[i]=entry;overlap=true}else{indices.set(key,items.length);items.push(entry);added.push(entry)}}
   if(overlap&&this.compare)items.sort(this.compare);
   this.items=items;this.meta=page;this.cursor=next;this.more=!!next;if(next)this.cursors.add(next);
   this.onUpdate(this.items,{meta:page,append:!!after&&!overlap,added,loading:false});
  }catch(e){if(epoch===this.epoch&&!this.destroyed&&!controller.signal.aborted){this.error=true;this.onError(e)}}
  finally{if(epoch===this.epoch&&!this.destroyed){this.busy=false;this.onLoading(false);this.status();if(!this.error&&this.more)requestAnimationFrame(()=>{if(epoch!==this.epoch||this.destroyed)return;const rect=this.sentinel.getBoundingClientRect(),bounds=this.root?.getBoundingClientRect()??{top:0,bottom:innerHeight};if(rect.top<bounds.bottom+400&&rect.bottom>=bounds.top)this.loadMore()})}}
 }
 status(){
  if(this.destroyed)return;
  if(this.error){this.sentinel.textContent='Weitere Einträge konnten nicht geladen werden. ';const button=document.createElement('button');button.type='button';button.className='text-button';button.textContent='Erneut versuchen';button.onclick=()=>this.loadMore(true);this.sentinel.append(button);return}
  this.sentinel.textContent=this.busy?'Einträge werden geladen …':this.items.length?`${this.items.length}${this.meta.total==null?'':' von '+this.meta.total} geladen`:'';
 }
 patch(entries){const byID=new Map(entries.map(e=>[this.key(e),e]));this.items=this.items.map(e=>byID.get(this.key(e))??e);this.onUpdate(this.items,{meta:this.meta,append:false,loading:false});this.status()}
 clear(){this.cancel();this.items=[];this.cursor='';this.more=false;this.error=false;this.meta={};this.cursors.clear();this.sentinel.textContent=''}
 destroy(){if(this.destroyed)return;this.clear();this.destroyed=true;this.observer.disconnect();this.sentinel.remove()}
}
// Shared debounce and lifecycle for existing inputs and plugin search controls.
export function bindSearch(input,load,{delay=250,onChange=()=>{}}={}){
 let timer;const change=()=>{clearTimeout(timer);onChange();timer=setTimeout(()=>load(input.value.trim()),delay)};input.addEventListener('input',change);
 return {flush(){clearTimeout(timer);return load(input.value.trim())},clear(){clearTimeout(timer)},destroy(){clearTimeout(timer);input.removeEventListener('input',change)}};
}
