import json,re,urllib.request,base64,struct,uuid,sys
from pathlib import Path
cfg=Path(sys.argv[1]).read_text();base=sys.argv[2];op=sys.argv[3]
user=re.search(r'username: (.+)',cfg)[1].strip();pw=re.search(r'password: (.+)',cfg)[1].strip()
auth='Basic '+base64.b64encode((user+':'+pw).encode()).decode()
def req(path,data=None):
 r=urllib.request.Request(base+path,data,headers={'Authorization':auth,'Content-Type':'application/octet-stream'})
 try:
  with urllib.request.urlopen(r,timeout=25) as f:return f.read()
 except urllib.error.HTTPError as e:raise RuntimeError(str(e)+repr(e.read()))
def v(n):
 b=b''
 while n>=128:b+=bytes([(n&127)|128]);n>>=7
 return b+bytes([n])
def text(s):b=s.encode();return v(len(b))+b
if op=='stats':print(req('/api/volumes').decode());sys.exit()
if op=='create':
 bid=uuid.uuid4().hex;body=bytes.fromhex(bid)+struct.pack('<Q',1)+text(sys.argv[4])+bytes(5)+struct.pack('<HHH',1,0,0)+b'\0'+struct.pack('<HQBQ?QI',0,0,0,0,True,0,540);code=0x2003
elif op=='list':body=b'\0'+struct.pack('<I',100);code=0x2022
elif op=='delete':body=bytes.fromhex(sys.argv[4]);code=0x2008
b=req('/api/message',struct.pack('<BBH',1,1,code)+uuid.uuid4().bytes+body)
print(json.dumps({'bucket':bid,'reply':b.hex()}) if op=='create' else repr(b[20:]))
