import os, py_compile, json
roots=[r"E:\\1_Data", r"E:\\vibe\\buffett"]
exclude_parts=[
    "\\backup\\","\\_backup\\","\\_tmp\\","\\2_Logs\\","\\runs\\_archive\\",
    "\\.git\\","\\__pycache__\\","\\_bulk_backup_","\\tools_archive\\","\\_notes\\"
]
errors=[]
files=[]
for root in roots:
    for dp,_,fns in os.walk(root):
        p=dp.replace('/','\\').lower()
        if any(x.lower() in p for x in exclude_parts):
            continue
        for fn in fns:
            lowfn=fn.lower()
            if not lowfn.endswith('.py'):
                continue
            if lowfn.startswith('backup_') or '.bak_' in lowfn or '.broken_' in lowfn or '.edit.py' in lowfn:
                continue
            full=os.path.join(dp,fn)
            files.append(full)
for p in files:
    try:
        py_compile.compile(p, doraise=True)
    except Exception as e:
        errors.append({'file':p,'error':str(e)})
out=r"E:\\1_Data\\_tmp_syntax_errors_operational_after.json"
with open(out,'w',encoding='utf-8') as f:
    json.dump({'total_py':len(files),'errors':errors,'error_count':len(errors)},f,ensure_ascii=False,indent=2)
print('TOTAL',len(files))
print('ERRORS',len(errors))
print('OUT',out)
