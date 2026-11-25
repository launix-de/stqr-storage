// SPDX-License-Identifier: GPL-3.0-only
export const versionWarning=v=>!!(v&&(v.outdated||v.incompatible||v.restricted));
export function versionLabel(v){
 if(v?.incompatible)return 'Version inkompatibel';
 if(v?.restricted)return 'Teilweise inkompatibel';
 if(v?.outdated)return 'Veraltetes Protokoll';
 return '';
}
export const protocolLabel=v=>`Protokoll ${v?.profile?.protocol_version??'unbekannt'}`;
