from pathlib import Path
s=Path('/tmp/stqr-throughput-benchmark/bench.py').read_text().split('\ntry:\n lower=',1)[0]
s='\n'.join(line for line in s.splitlines() if not line.startswith('with urllib.request.urlopen('))
exec(compile(s,'bench.py','exec'))
rows=json.loads((W/'results.json').read_text());B=json.loads((W/'buckets.json').read_text());lower=B['lower']
try:
 root=mount('final_ro',lower,kind='ro',improved=True);run('final_ro',root,False);unmount('final_ro')
 b=create('final_rw');root=mount('final_rw',b,improved=True);prepare(root);unmount('final_rw');root=mount('final_rw',b,improved=True);run('final_rw',root,True);unmount('final_rw')
 b=create('final_overlay');root=mount('final_overlay',lower,kind='overlay',upper=b,improved=True);run('final_overlay',root,True);unmount('final_overlay')
finally:
 for name in list(processes):unmount(name)
print('FINAL DONE',flush=True)
