import requests
from sqlalchemy import create_engine
import pandas as pd
import yaml
import re

def clean_column_names(df):
    """Nettoyer les noms de colonnes d'un DataFrame."""
    regex = r'[^a-zA-Z0-9_]+'
    new_columns = [re.sub(regex, '_', col.strip()) for col in df.columns]
    df.columns = new_columns
    
    return df

def store_excel(files):
    with open('Ressources/configDB.yml', 'r') as f:
        configDB = yaml.safe_load(f)
    config = configDB.get('mysql', [])
    engine = create_engine(f"mysql://{config['user']}:{config['password']}@{config['host']}/{config['database']}")
    
    for file in files:
        xl_file = pd.ExcelFile(file)
        for sheet_name in xl_file.sheet_names:
            sheet_data = xl_file.parse(sheet_name)
            if sheet_data.columns.nlevels > 1:
                continue
            else:
                df = clean_column_names(sheet_data)
                query = f"SELECT MAX(id) FROM {sheet_name}"
                max_id_df = pd.read_sql_query(query, engine)
                max_id = max_id_df.iloc[0, 0]
                if pd.isna(max_id):
                    start_id = 1
                else:
                    start_id = int(max_id) + 1
                ids = range(start_id, start_id + len(df))
                df['id'] = ids   
                df.head()
                df.to_sql(sheet_name, engine, if_exists="append", index=False)
                auth_url = 'http://localhost:8080/api/v1/auth/authenticate'
                auth_payload = {'email': 'archimaster@orange.com', 'password': '123'}
                auth_response = requests.post(auth_url, json=auth_payload)
                if auth_response.status_code == 200:
                    token = auth_response.json().get('token')
                else:
                    return 'Failed to authenticate'

                headers = {'Authorization': f'Bearer {token}'}
                if(sheet_name == 'applications'):
                      for id in ids :
                        url_assessment = f"http://127.0.0.1:8080/api/v1/applications/{id}/addAssessment/1"
                        requests.post(url_assessment,headers=headers)
    
    return "File(s) Upload Successful"
