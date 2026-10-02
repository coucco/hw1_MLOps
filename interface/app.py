import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
import uuid
import altair as alt
import psycopg2

KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "fraud"),
    "user": os.getenv("POSTGRES_USER", "fraud"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud")
}

def load_file(uploaded_file):
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def fetch_results():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT transaction_id, score, fraud_flag FROM scores "
                "WHERE fraud_flag = 1 ORDER BY id DESC LIMIT 10"
            )
            fraud_rows = cur.fetchall()
            cur.execute("SELECT score FROM scores ORDER BY id DESC LIMIT 100")
            score_rows = cur.fetchall()
    finally:
        conn.close()
    fraud_df = pd.DataFrame(fraud_rows, columns=["transaction_id", "score", "fraud_flag"])
    scores_df = pd.DataFrame(score_rows, columns=["score"])
    return fraud_df, scores_df

def send_to_kafka(df, topic, bootstrap_servers):
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )

        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]

        progress_bar = st.progress(0)
        total_rows = len(df)

        for idx, row in df.iterrows():

            producer.send(
                topic,
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)

        producer.flush()

        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

st.title("Отправка данных в Kafka")

uploaded_file = st.file_uploader(
    "Загрузите CSV файл с транзакциями",
    type=["csv"]
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:

    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Загружен",
        "df": load_file(uploaded_file)
    }
    st.success(f"Файл {uploaded_file.name} успешно загружен!")

if st.session_state.uploaded_files:
    st.subheader("Список загруженных файлов")

    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])

        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}`")
            st.markdown(f"**Статус:** `{file_data['status']}`")

        with cols[2]:
            if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"]
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()
                else:
                    st.error("Файл не содержит данных")

st.divider()
st.title("Результаты скоринга")

if st.button("Посмотреть результаты"):
    try:
        fraud_df, scores_df = fetch_results()
    except Exception as e:
        st.error(f"Ошибка чтения из базы данных: {str(e)}")
    else:
        st.subheader("Последние 10 транзакций с fraud_flag = 1")
        if fraud_df.empty:
            st.info("Транзакций с fraud_flag = 1 пока нет")
        else:
            st.dataframe(fraud_df, use_container_width=True)

        st.subheader(f"Распределение скоров последних транзакций ({len(scores_df)})")
        if scores_df.empty:
            st.info("В базе пока нет транзакций")
        else:
            chart = alt.Chart(scores_df).mark_bar().encode(
                alt.X("score:Q", bin=alt.Bin(maxbins=20), title="score"),
                alt.Y("count()", title="Количество транзакций")
            )
            st.altair_chart(chart, use_container_width=True)
