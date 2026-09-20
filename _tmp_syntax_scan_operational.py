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
            if fn.lower().endswith('.py'):
                full=os.path.join(dp,fn)
                low=full.replace('/','\\').lower()
                if '.bak_' in low or '.broken_' in low or '.edit.py' in low or low.endswith('.py.broken'):
                    continue
                files.append(full)
for p in files:
    try:
        py_compile.compile(p, doraise=True)
    except Exception as e:
        errors.append({'file':p,'error':str(e)})
out=r"E:\\1_Data\\_tmp_syntax_errors_operational.json"
with open(out,'w',encoding='utf-8') as f:
    json.dump({'total_py':len(files),'errors':errors,'error_count':len(errors)},f,ensure_ascii=False,indent=2)
print('TOTAL',len(files))
print('ERRORS',len(errors))
print('OUT',out)
