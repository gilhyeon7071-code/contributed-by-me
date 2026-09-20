import pandas as pd

df = pd.read_csv('E:/1_Data/2_Logs/candidates_latest_data.with_final_score.csv')
affected = df[(df['news_score'] < 0) | (df['news_topic_l3_execution_allowed'] == False)]
affected[['code', 'name', 'news_score', 'news_topic_candidate_actions', 'news_topic_l3_execution_allowed']].to_csv('E:/1_Data/2_Logs/e2e_shadow.csv', index=False, encoding='utf-8-sig')
