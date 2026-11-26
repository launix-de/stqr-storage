// SPDX-License-Identifier: GPL-3.0-only
// Clipboard API is unavailable on ordinary intranet HTTP. Keep a user-gesture
// fallback so copying identifiers does not depend on TLS or browser flags.
export async function copyText(text) {
 if(navigator.clipboard?.writeText){try{await navigator.clipboard.writeText(text);return}catch{}}
 const active=document.activeElement,selection=window.getSelection(),ranges=[];
 if(selection)for(let i=0;i<selection.rangeCount;i++)ranges.push(selection.getRangeAt(i).cloneRange());
 const input=document.createElement('textarea');input.value=text;input.readOnly=true;input.style.cssText='position:fixed;left:-9999px;top:0';document.body.append(input);
 try{input.select();if(!document.execCommand('copy'))throw Error('Kopieren nicht möglich. Bitte die ID markieren und manuell kopieren.')}finally{input.remove();active?.focus({preventScroll:true});if(selection){selection.removeAllRanges();for(const range of ranges)if(range.startContainer.isConnected)selection.addRange(range)}}
}
