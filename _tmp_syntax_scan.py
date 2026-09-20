import os, py_compile, json
roots=[r"E:\\1_Data", r"E:\\vibe\\buffett"]
errors=[]
files=[]
for root in roots:
    for dp,_,fns in os.walk(root):
        for fn in fns:
            if fn.lower().endswith('.py'):
                p=os.path.join(dp,fn)
                files.append(p)
for p in files:
    try:
        py_compile.compile(p, doraise=True)
    except Exception as e:
        errors.append({'file':p,'error':str(e)})
out=r"E:\\1_Data\\_tmp_syntax_errors.json"
with open(out,'w',encoding='utf-8') as f:
    json.dump({'total_py':len(files),'errors':errors,'error_count':len(errors)},f,ensure_ascii=False,indent=2)
print('TOTAL',len(files))
print('ERRORS',len(errors))
print('OUT',out)
