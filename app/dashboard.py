"""Read only the prediction store populated by the separate Kafka consumer."""
import json
import os
from pathlib import Path
import sqlite3
import pandas as pd
import streamlit as st

# Official CPCB AQI categories (same cut points as final_ml.aqi_category).
CATEGORY_BINS = [-float('inf'), 50, 100, 200, 300, 400, float('inf')]
CATEGORY_LABELS = ['Good', 'Satisfactory', 'Moderate', 'Poor', 'Very Poor', 'Severe']

st.set_page_config(page_title='India | AQI estimation stream',page_icon='🌏',layout='wide')
st.title('India · AQI estimation stream')
st.caption('Historical replay · Five cities · Same-day AQI estimated from pollutant readings · Kafka → inference → prediction store')
st.info('Academic simulation using 2015–2024 records. These estimates are not current observations or health guidance.')
store=Path(os.getenv('PREDICTION_STORE','runtime/predictions.sqlite'))
artifacts=Path(os.getenv('MODEL_ARTIFACTS','artifacts'))
if (artifacts/'metadata.json').exists():
    meta=json.loads((artifacts/'metadata.json').read_text(encoding='utf-8'))
    with st.expander('Model and evaluation'):
        st.write('Model:',meta.get('model_name'),'· version',meta.get('model_version'))
        test=meta.get('metrics',{}).get('test',{}).get('selected_model',{})
        if test:
            m1,m2,m3=st.columns(3)
            m1.metric('Test MAE (AQI points)',f"{test.get('mae',float('nan')):.2f}")
            m2.metric('Test R²',f"{test.get('r2',float('nan')):.3f}")
            m3.metric('Category accuracy',f"{test.get('category_accuracy',float('nan')):.1%}")
        st.write('Each Kafka message carries the pollutant readings (PM2.5, PM10, NO, NO2, NOx, NH3, CO, SO2, O3) of one city and day. '
                 'The model estimates the AQI of that same day; the observed AQI is never sent in the message.')
        st.json(meta.get('metrics',{}))
city=st.selectbox('City',['All','Chennai','Delhi','Kolkata','Mumbai'])

@st.fragment(run_every='2s')
def live():
    if not store.exists():
        st.warning('Waiting for the Kafka consumer. Start the consumer, then the producer.');return
    with sqlite3.connect(store.resolve().as_uri()+'?mode=ro',uri=True,timeout=5) as conn:
        total=conn.execute('SELECT COUNT(*) FROM predictions').fetchone()[0]
        rejected=conn.execute('SELECT COUNT(*) FROM rejected_events').fetchone()[0]
        rows=pd.read_sql_query('SELECT event_id,city_name,target_date,predicted_aqi,model_version,processed_at,topic,partition_id,kafka_offset FROM predictions ORDER BY processed_at DESC LIMIT 1000',conn)
    a,b,c=st.columns(3)
    a.metric('Estimates stored',f'{total:,}')
    b.metric('Messages rejected',rejected)
    c.metric('Last processed (UTC)',rows.processed_at.iloc[0][11:19] if len(rows) else 'Waiting')
    if rows.empty:
        st.write('Connected to the prediction store; no messages processed yet.');return
    rows['estimated_category']=pd.cut(rows.predicted_aqi,bins=CATEGORY_BINS,labels=CATEGORY_LABELS).astype(str)
    view=rows if city=='All' else rows[rows.city_name==city]
    if view.empty:st.write('No estimates for this city yet.');return
    view=view.sort_values(['target_date','processed_at'])
    left,right=st.columns([3,2])
    with left:
        chart=view.drop_duplicates(['target_date','city_name'],keep='last').pivot(index='target_date',columns='city_name',values='predicted_aqi')
        st.subheader('Estimated AQI by measurement date')
        st.line_chart(chart,x_label='Measurement date (India local day)',y_label='Estimated AQI')
    with right:
        st.subheader('Estimated category')
        counts=view.estimated_category.value_counts().reindex(CATEGORY_LABELS,fill_value=0)
        st.bar_chart(counts,x_label='CPCB category',y_label='Messages')
    st.subheader('Latest Kafka estimates')
    columns=['city_name','target_date','predicted_aqi','estimated_category','model_version','processed_at','topic','partition_id','kafka_offset','event_id']
    st.dataframe(view.sort_values('processed_at',ascending=False)[columns],hide_index=True,width='stretch')
    st.caption('This page reads only consumer output. Kafka offsets, model version and processing time provide message provenance. Last 1,000 estimates shown; counters cover the full store.')

live()
