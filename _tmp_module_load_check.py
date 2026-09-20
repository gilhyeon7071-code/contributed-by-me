import importlib.util
mods=[
 r"E:\\vibe\\buffett\\dashboard_recovered.py",
 r"E:\\vibe\\buffett\\vibe_v18.py",
 r"E:\\vibe\\buffett\\pages\\00_Health_Status.py",
]
for p in mods:
    try:
        spec=importlib.util.spec_from_file_location("m", p)
        m=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        print("LOAD_OK", p)
    except Exception as e:
        print("LOAD_FAIL", p, type(e).__name__, e)
        raise
