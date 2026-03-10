@echo off
cd /d E:\1_Data
python -c "import json; j=json.load(open(r'2_Logs\after_close_summary_last.json','r',encoding='utf-8')); buys=((j.get('executions') or {}).get('buys') or []); out=['BUY_COUNT='+str(len(buys))]; [out.append(str(i)+'. '+str((b or {}).get('code'))+' '+str((b or {}).get('name'))+' ['+str((b or {}).get('market'))+'] score='+str((b or {}).get('score'))+' '+str(((b or {}).get('reason') or (b or {}).get('why') or '')).strip()) for i,b in enumerate(buys,1)]; open(r'2_Logs\buy_list_last.txt','w',encoding='utf-8').write('\n'.join(out)); print('OK: wrote 2_Logs\\buy_list_last.txt')"
type 2_Logs\buy_list_last.txt
