import streamlit as st
import base64
import pandas as pd

df=pd.read_csv("20228_1.csv")

df.rename(columns={"Latitude In":"lat","Longitude In":"lon"},inplace=True)

df=df.loc[df["lat"]!=0]

st.map(df[["lat","lon"]])