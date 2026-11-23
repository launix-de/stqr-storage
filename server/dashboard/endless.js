// SPDX-License-Identifier: GPL-3.0-only
// Fetch one cursor page at a time. Off-screen lists never drain their API pages.
export class EndlessList {
 constructor({container,fetchPage,entries,onUpdate,onError=()=>{},onLoading=()=>{},visible=()=>true,size=50}){
  Object.assign(this,{fetchPage,entries,onUpdate,onError,onLoading,visible,size});this.items=[];this.cursor='';this.more=false;this.epoch=0;this.busy=false;this.meta={};
  this.sentinel=document.createElement('div');this.sentinel.className='endless-status';this.sentinel.setAttribute('role','status');container.append(this.sentinel);
  this.observer=new IntersectionObserver(entries=>{if(entries.some(e=>e.isIntersecting))this.loadMore()},{rootMargin:'400px'});this.observer.observe(this.sentinel);
 }
 async reset(query=''){
  this.epoch++;this.query=query;this.items=[];this.cursor='';this.more=true;this.busy=false;this.error=false;this.onUpdate(this.items,{meta:this.meta,append:false,loading:true});return this.loadMore(true);
 }
 async loadMore(initial=false){
  if(this.busy||!this.more||!initial&&!this.visible())return;
  const epoch=this.epoch,after=this.cursor;this.busy=true;this.error=false;this.onLoading(true);this.status();
  try{
   const page=await this.fetchPage(after,this.size,this.query);if(epoch!==this.epoch)return;
   const next=page.next??'';if(next&&next===after)throw Error('Ungültiger Folgecursor.');
   const incoming=this.entries(page),indices=new Map(this.items.map((entry,i)=>[entry.id,i]));let overlap=false;const added=[];
   for(const entry of incoming){const i=indices.get(entry.id);if(i!==undefined){this.items[i]=entry;overlap=true}else{indices.set(entry.id,this.items.length);this.items.push(entry);added.push(entry)}}
   this.meta=page;this.cursor=next;this.more=!!next;
   if(overlap)this.items.sort((a,b)=>a.name<b.name?-1:a.name>b.name?1:0);
   this.onUpdate(this.items,{meta:page,append:!!after&&!overlap,added,loading:false});
  }catch(e){if(epoch===this.epoch){this.error=true;this.onError(e)}}
  finally{if(epoch===this.epoch){this.busy=false;this.onLoading(false);this.status();if(!this.error&&this.more)requestAnimationFrame(()=>{const rect=this.sentinel.getBoundingClientRect();if(rect.top<innerHeight+400&&rect.bottom>=0)this.loadMore()})}}
 }
 status(){
  if(this.error){this.sentinel.textContent='Weitere Einträge konnten nicht geladen werden. ';const button=document.createElement('button');button.type='button';button.className='text-button';button.textContent='Erneut versuchen';button.onclick=()=>this.loadMore(true);this.sentinel.append(button);return}
  this.sentinel.textContent=this.busy?'Einträge werden geladen …':this.items.length?`${this.items.length} von ${this.meta.total??this.items.length} geladen`:'';
 }
 patch(entries){const byID=new Map(entries.map(e=>[e.id,e]));this.items=this.items.map(e=>byID.get(e.id)??e);this.onUpdate(this.items,{meta:this.meta,append:false,loading:false});this.status()}
 clear(){this.epoch++;this.items=[];this.cursor='';this.more=false;this.busy=false;this.error=false;this.meta={};this.sentinel.textContent=''}
}
