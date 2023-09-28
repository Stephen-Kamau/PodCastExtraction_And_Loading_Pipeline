import requests
import xmltodict
import pendulum
import sqlite3
from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
EPISODE_FOLDER = "./episodes"

PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"



# function to load the episodes to db
def load_episodes(episodes):
    conn = sqlite3.connect('episodes.db') 
    cursor = conn.cursor()
    stored_episodes = cursor.execute("SELECT * from episodes;").fetchall()
    new_episodes = []
    for episode in episodes:
        link = episode["link"]
        if link not in [row[0] for row in stored_episodes]:
            filename = f"{link.split('/')[-1]}.mp3"
            new_episodes.append((link, episode["title"], filename, episode["pubDate"], episode["description"], ""))
    
    cursor.executemany("INSERT INTO episodes (link, title, filename, published, description, transcript) VALUES (?, ?, ?, ?, ?, ?)", new_episodes)
    conn.commit()
    conn.close()
    
# function to create db
def create_table():
        conn = sqlite3.connect('episodes.db')  
        cursor = conn.cursor()
        create_sql = """
            CREATE TABLE IF NOT EXISTS episodes (
                link TEXT PRIMARY KEY,
                title TEXT,
                filename TEXT,
                published TEXT,
                description TEXT,
                transcript TEXT
            );
        """
        cursor.execute(create_sql)
        conn.commit()
        conn.close()



# function to get episodes
def get_episodes():
    data = requests.get(PODCAST_URL)
    feed = xmltodict.parse(data.text)
    episodes = feed["rss"]["channel"]["item"]
    print(f"Found {len(episodes)} episodes.")
    return episodes


def download_episodes(episodes):
    if not os.path.isdir(EPISODE_FOLDER):
        print(f"Path Not found, Making it")
        os.mkdir(EPISODE_FOLDER)
    for episode in episodes:
        name_end = episode["link"].split('/')[-1]
        filename = f"{name_end}.mp3"
        audio_path = os.path.join(EPISODE_FOLDER, filename)
        print(f"Audio Path is  {audio_path}")
        
        if not os.path.exists(audio_path):
            print(f"Downloading {filename}")
            audio = requests.get(episode["enclosure"]["@url"])
            with open(audio_path, "wb+") as f:
                f.write(audio.content)

# Define the Airflow DAG
dag = DAG(
    dag_id='podcast_summary',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 5, 30),
    catchup=False,
)

# Create task objects using PythonOperator
create_table_task = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table,
    dag=dag,
)

get_episodes_task = PythonOperator(
    task_id='get_episodes_task',
    python_callable=get_episodes,
    dag=dag,
)

load_episodes_task = PythonOperator(
    task_id='load_episodes_task',
    python_callable=load_episodes,
    op_args=[get_episodes_task.output],  # Pass the output of get_episodes_task to load_episodes_task
    dag=dag,
)

download_episodes_task = PythonOperator(
    task_id='download_episodes_task',
    python_callable=download_episodes,
    op_args=[get_episodes_task.output],  # Pass the output of get_episodes_task to download_episodes_task
    dag=dag,
)

# Define the task dependencies
create_table_task >> get_episodes_task >> [load_episodes_task, download_episodes_task]