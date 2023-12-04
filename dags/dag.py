from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from bs4 import BeautifulSoup
import requests
import pandas as pd

# Configurar parâmetros da DAG
default_args = {
    'owner': 'natalia_vieira',
    'depends_on_past': False,
    'start_date': days_ago(1),
}


def extrair_dados_G1(**kwargs):
    ''' Extrai os dados de link e título das notícias da página principal do G1 '''
    g1_url = 'https://g1.globo.com'
    response = requests.get(g1_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    news_data = []
    for post in soup.find_all('div', class_='bastian-feed-item'):
        #Extrai título da notícia
        title = post.find('a', class_='feed-post-link').text.strip() 
        #Extrai link da notícia
        link = post.find('a', class_='feed-post-link')['href']
        news_data.append({'title': title, 'link': link})

    #Salvo as informações em uma planilha
    df = pd.DataFrame(news_data)

    #Planilha é salva no formato .csv
    caminho = '/home/natalialinux/projetos/desafio_airflow' #inserir aqui o caminho onde o arquivo será salvo
    csv_path = f'{caminho}/g1_news.csv'
    df.to_csv(csv_path, index=False)


dag = DAG(
    'dag_desafio_airflow',
    default_args=default_args,
    description='DAG de extração de dados de notícias do G1',
    schedule_interval=timedelta(hours=1),  #Executa de 1 em 1 hora
)

extrair_dados_G1_task = PythonOperator(
    task_id='extrair_dados_G1',
    python_callable=extrair_dados_G1,
    provide_context=True,
    dag=dag,
)