import streamlit as st  # 🎈 data web app development
import pandas as pd  # read csv, df manipulation


page_title = "Main property page"
page_icon = ":house_with_garden:"
layout = "wide"
st.set_page_config(page_title=page_title, page_icon=page_icon, layout=layout)
st.header(page_title + " " + page_icon)

df = pd.read_json("timeprice.json")

st.dataframe(df)
st.line_chart(df, x="time", y="price")
