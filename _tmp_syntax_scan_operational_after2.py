import os, py_compile, json
roots=[r"E:\\1_Data", r"E:\\vibe\\buffett"]
errors=[]
files=[]
for root in roots:
    for dp,_,fns in os.walk(root):
        p=dp.replace('/','\\').lower()
        if any(k in p for k in ["\\backup\\","\\_backup\\","\\_tmp\\","\\2_logs\\","\\_notes\\","\\tools_archive\\","\\.git\\","\\__pycache__\\"]):
            continue
        for fn in fns:
            low=fn.lower()
            if not low.endswith('.py'):
                continue
            if low.startswith('backup_') or '.bak_' in low or '.broken_' in low or '.edit.py' in low:
                continue
            full=os.path.join(dp,fn)
            lf=full.replace('/','\\').lower()
            if "\\_notes\\" in lf or "\\2_logs\\" in lf:
                continue
            files.append(full)
for p in files:
    try:
        py_compile.compile(p, doraise=True)
    except Exception as e:
        errors.append({'file':p,'error':str(e)})
out=r"E:\\1_Data\\_tmp_syntax_errors_operational_after2.json"
with open(out,'w',encoding='utf-8') as f:
    json.dump({'total_py':len(files),'errors':errors,'error_count':len(errors)},f,ensure_ascii=False,indent=2)
print('TOTAL',len(files))
print('ERRORS',len(errors))
print('OUT',out)
