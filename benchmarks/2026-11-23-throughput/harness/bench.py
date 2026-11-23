import os,time,json,re,subprocess,base64,urllib.request,statistics,random
from pathlib import Path
W=Path('/tmp/stqr-throughput-benchmark');D=Path('/home/carli/.cache/stqr-throughput-20261123');CFG=D/'stqr.yaml';URL='http://127.0.0.1:17421'
c=CFG.read_text();user=re.search(r'username: (.+)',c)[1].strip();pw=re.search(r'password: (.+)',c)[1].strip();headers={'Authorization':'Basic '+base64.b64encode((user+':'+pw).encode()).decode(),'Content-Type':'application/json','Idempotency-Key':__import__('uuid').uuid4().hex}
with urllib.request.urlopen(urllib.request.Request(URL+'/api/volumes',json.dumps({'name':'data','source':0,'class':'hdd','size_bytes':2<<30}).encode(),headers=headers),timeout=30) as r:r.read()
rows=[];processes={};B={};N=128;SIZE=32<<20;block=os.urandom(1<<20);script=b'<?php return 7; /*';script+=b'x'*(4096-len(script)-3)+b'*/\n'
def create(name):
 p=subprocess.run(['python3',str(W/'admin.py'),str(CFG),URL,'create','throughput-'+name],capture_output=True,text=True,check=True);b=json.loads(p.stdout)['bucket'];B[name]=b;(W/'buckets.json').write_text(json.dumps(B));return b
def mount(name,bucket,kind='rw',improved=False,upper=None):
 p=W/name;p.mkdir(exist_ok=True);args=[str(W/('stqr-fuse' if improved else 'baseline-fuse')),'--uri',URL,'--username',user,'--password',pw]
 if kind=='ro':args+=['--readonly-bucket',bucket]
 elif kind=='overlay':args+=['--readonly-bucket',bucket,'--bucket',upper,'--init']
 else:args+=['--bucket',bucket,'--init']
 f=open(W/(name+'.log'),'a');proc=subprocess.Popen(args+[str(p)],stdout=f,stderr=f);processes[name]=(proc,p,f)
 for _ in range(400):
  if os.path.ismount(p):return p
  if proc.poll()!=None:raise RuntimeError('mount failed '+name)
  time.sleep(.025)
 raise RuntimeError('mount timeout '+name)
def unmount(name):
 proc,path,f=processes.pop(name)
 if os.path.ismount(path):subprocess.run(['fusermount','-u',str(path)],check=True)
 proc.wait(timeout=20);f.close()
def rec(case,test,fn,units,unit,rep):
 t=time.perf_counter();fn();elapsed=time.perf_counter()-t;row=dict(case=case,test=test,rep=rep,seconds=elapsed,units=units,unit=unit,rate=units/elapsed);rows.append(row);(W/'results.json').write_text(json.dumps(rows,indent=2));print(f'{case:20} {test:20} {rep}: {units/elapsed:.2f} {unit}/s ({elapsed:.3f}s)',flush=True)
def write(path):
 fd=os.open(path,os.O_CREAT|os.O_TRUNC|os.O_WRONLY,0o600)
 try:
  for _ in range(SIZE//len(block)):
   left=memoryview(block)
   while left:
    n=os.write(fd,left);assert n>0;left=left[n:]
  os.fsync(fd)
 finally:os.close(fd)
def prepare(root):
 (root/'php').mkdir(exist_ok=True)
 for i in range(N):(root/'php'/f'f{i}.php').write_bytes(script)
 write(root/'data.bin')
def read(path):
 with open(path,'rb',buffering=0) as f:
  n=0
  while b:=f.read(1<<20):n+=len(b)
 assert n==SIZE
php='''<?php $root=$argv[1];$times=[];for($r=0;$r<4;$r++){clearstatcache();$t=hrtime(true);$s=0;for($i=0;$i<128;$i++)$s+=require $root.'/php/f'.$i.'.php';if($s!==896)exit(4);$times[]=(hrtime(true)-$t)/1e9;}echo json_encode($times);''';(W/'php.php').write_text(php)
def run(case,root,writable):
 print('BEGIN '+case,flush=True)
 for rep in range(3):
  rec(case,'seq_read',lambda:read(root/'data.bin'),32,'MiB',rep)
  paths=[root/'php'/f'f{i}.php' for i in range(N)]
  def stats():
   for _ in range(4):
    for p in paths:assert p.stat().st_size==4096
  rec(case,'stat',stats,512,'ops',rep)
  def missing():
   for _ in range(4):
    for i in range(N):assert not(root/'php'/f'missing-{i}').exists()
  rec(case,'stat_missing',missing,512,'ops',rep)
  def opens():
   for p in paths:
    with open(p,'rb')as f:assert f.read()==script
  rec(case,'open_read_close',opens,128,'files',rep)
  if writable:
   rec(case,'seq_write_fsync',lambda:write(root/f'write-{rep}.bin'),32,'MiB',rep)
   directory=root/f'create-{rep}';directory.mkdir()
   def creates():
    for i in range(64):
     with open(directory/f'f{i}','wb',buffering=0)as f:f.write(script)
   rec(case,'create_close',creates,64,'files',rep)
   def syncs():
    for i in range(32):
     with open(directory/f'sync-{i}','wb',buffering=0)as f:f.write(script);os.fsync(f.fileno())
   rec(case,'create_fsync',syncs,32,'files',rep)
   def deletes():
    for p in list(directory.iterdir()):p.unlink()
    directory.rmdir()
   rec(case,'unlink',deletes,96,'files',rep)
 for freq in [0,2]:
  for rep in range(3):
   p=subprocess.run(['php','-d','opcache.enable_cli=1','-d','opcache.file_update_protection=0','-d','opcache.validate_timestamps=1','-d',f'opcache.revalidate_freq={freq}',str(W/'php.php'),str(root)],capture_output=True,text=True,check=True);times=json.loads(p.stdout);(W/f'php-{case}-{freq}-{rep}.json').write_text(json.dumps(times))
   for phase,sec,count in [('first',times[0],128),('warm',sum(times[1:]),384)]:rows.append(dict(case=case,test=f'php_freq{freq}_{phase}',rep=rep,seconds=sec,units=count,unit='includes',rate=count/sec))
   (W/'results.json').write_text(json.dumps(rows,indent=2));print(f'{case} PHP freq={freq}: first={times[0]*1000:.2f}ms warm={statistics.median(times[1:])*1000:.2f}ms',flush=True)
 print('END '+case,flush=True)
try:
 lower=create('lower');root=mount('seed',lower);print('PREPARE LOWER',flush=True);prepare(root);unmount('seed')
 for improved in [False,True]:
  case=('improved' if improved else 'baseline')+'_ro';root=mount(case,lower,kind='ro',improved=improved);run(case,root,False);unmount(case)
 for improved in [False,True]:
  case=('improved' if improved else 'baseline')+'_rw';bucket=create(case);root=mount(case,bucket,improved=improved);print('PREPARE '+case,flush=True);prepare(root);unmount(case);root=mount(case,bucket,improved=improved);run(case,root,True);unmount(case)
 for improved in [False,True]:
  case=('improved' if improved else 'baseline')+'_overlay';upper=create(case);root=mount(case,lower,kind='overlay',upper=upper,improved=improved);run(case,root,True);unmount(case)
finally:
 for name in list(processes):unmount(name)
print('DONE',flush=True)
