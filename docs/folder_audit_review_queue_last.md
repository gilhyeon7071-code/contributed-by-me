# Review Queue (last)

## G3 Isolated (code exists but no RootB ref\_abs)

* count: 3
* \_dev (class=DEV\_OR\_DATA, maturity=L2(runnable-candidate), ref\_abs=0, ref\_name=0)
* \_notes (class=DOCS, maturity=L2(runnable-candidate), ref\_abs=0, ref\_name=0)
* utils (class=TOOLS, maturity=L1(code-but-no-entry/docs/tests), ref\_abs=0, ref\_name=2)



## G5 UNKNOWN (needs labeling, do not delete without CR)

* count: 11
* **pycache** (maturity=L0(no-code), ref\_abs=0, ref\_name=0)
* .claude (maturity=L0(no-code), ref\_abs=0, ref\_name=0)
* '') (maturity=L0(no-code), ref\_abs=0, ref\_name=0)
* '').replace('-' (maturity=L0(no-code), ref\_abs=0, ref\_name=0)
* (report\['meta'].get('latest\_date') (maturity=L0(no-code), ref\_abs=0, ref\_name=0)
* 새 폴더 (maturity=L0(no-code), ref\_abs=0, ref\_name=0)
* Macro (maturity=L0(no-code), ref\_abs=0, ref\_name=1)
* mx (maturity=L0(no-code), ref\_abs=0, ref\_name=0)
* None (maturity=L0(no-code), ref\_abs=0, ref\_name=778)
* or (maturity=L0(no-code), ref\_abs=0, ref\_name=0)
* pq.ParquetFile(str(p)).metadata (maturity=L0(no-code), ref\_abs=0, ref\_name=0)

\## Decision Record (manual append)

\### G3 Isolated — selector\_v0\_1 (E:\\1\_Data\\\_dev\\selector\_v0\_1)

\- Decision: ISOLATED\_KEEP + label=DEV\_OUTPUT\_ARTIFACTS

\- Evidence (dir listing, recurse+force):

&nbsp; - candidates\_scored\_20260113.csv

&nbsp; - orders\_20260113\_eval.xlsx

\- Rationale:

&nbsp; - 코드/엔트리/README 미관측이며, 산출물 2개 파일만 존재하는 보관 폴더로 관측됨.

&nbsp; - 운영 편입(연결 생성)은 트리거/CR 없이 금지이므로 격리 유지가 정합.

