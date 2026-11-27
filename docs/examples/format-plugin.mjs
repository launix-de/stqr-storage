// SPDX-License-Identifier: GPL-3.0-only
// A minimal third-party module example. Remove or replace this file when packaging.
export const apiVersion=1;
export const css='[data-stqr-plugin="example"] p { line-height: 1.6; }';
export function open({bucket,windows,signal}){const window=windows.open({title:bucket.name,signal}),p=document.createElement('p');p.textContent='Dieses Beispiel enthält keine Formatinterpretation.';window.root.append(p)}
