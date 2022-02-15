import pandas as pd
import  sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from datetime import datetime
import psycopg2
import pyodbc

engine = create_engine('sqlite:///chinook.db')

df_customers = pd.read_sql_query("""SELECT FirstName, LastName, Company, Address, City, State, Country,PostalCode, Phone, Fax,Email FROM customers; """, con=engine.connect())
df_artists = pd.read_sql_query("""SELECT Name FROM artists;""", con=engine.connect())
df_albums = pd.read_sql_query("""SELECT Title FROM albums;""", con=engine.connect())
df_genres = pd.read_sql_query("""SELECT Name FROM genres;""", con=engine.connect())
df_employees = pd.read_sql_query("""SELECT LastName, FirstName, Title, BirthDate, HireDate, Address, City, State, Country, PostalCode, Phone, Fax, Email FROM employees;""", con=engine.connect())
df_invoice_items = pd.read_sql_query("""SELECT UnitPrice, Quantity FROM invoice_items;""", con=engine.connect())
df_playlists = pd.read_sql_query("""SELECT Name FROM playlists;""", con=engine.connect()) 
df_location = pd.read_sql_query("""select BillingAddress, BillingCity, BillingState, BillingCountry,BillingPostalCode from invoices;""", con=engine.connect())
conn = sqlalchemy.create_engine('mssql+pyodbc://localhost/DW_Sales_Music?driver=SQL+Server+Native+Client+11.0')

df_albums.to_sql(name='Dim_Album', con=conn, if_exists='append',index=False)
df_artists.to_sql(name='Dim_Artist', con=conn, if_exists='append',index=False)
df_customers.to_sql(name='Dim_Customer', con=conn, if_exists='append',index=False) 
df_genres.to_sql(name='Dim_Genre', con=conn, if_exists='append',index=False)
df_invoice_items.to_sql(name='Dim_Invoice_Item', con=conn, if_exists='append',index=False)
df_location.to_sql(name='Dim_Location', con=conn, if_exists='append',index=False)
df_playlists.to_sql(name='Dim_Playlist', con=conn, if_exists='append',index=False)
df_employees.to_sql(name='Dim_Employee', con=conn, if_exists='append',index=False)