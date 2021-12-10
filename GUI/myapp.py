import streamlit as st
import pandas as pd
from math import cos, asin, sqrt, pi
import datetime
from datetime import timedelta
from dask import dataframe as dd
from haversine import haversine, Unit
import sys
import streamlit as st
from datetime import date
import time
import json



### Tracing Code
class Computer:
    def __init__(self, ClientMacAddr, lat, lng, localtime):
        date = localtime + ':00'  # '2021-05-21 11:22:03'
        datem = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        self.localtime = datem
        self.timestamp = int(datem.timestamp())
        self.lng = lng
        self.lat = lat
        self.ClientMacAddr = ClientMacAddr

    def find_distance(self, lat2, lon2):
        lat1, lon1 = self.lat, self.lng

        from haversine import haversine, Unit
        origin = (lat1, lon1)
        paris = (lat2, lon2)
        d1_feet = haversine(origin, paris, unit=Unit.FEET)

        return d1_feet

##### contact tracing code
def find_match(query_mac_position, max_distance_feet, contact_time_min):
    query_computer = computers[query_mac_position]
    query_mac_address = query_computer.ClientMacAddr

    max_time_limit = query_computer.timestamp + contact_time_min * 60
    ts_mac_map = {}
    for i in range(query_mac_position, len(computers)):
        ref_computer = computers[i]
        if ref_computer.timestamp > max_time_limit:
            break
        if query_mac_address == ref_computer.ClientMacAddr:
            continue
        distance = query_computer.find_distance(ref_computer.lat, ref_computer.lng)
        if distance > max_distance_feet:
            continue

        if ref_computer.timestamp not in ts_mac_map.keys():
            ts_mac_map[ref_computer.timestamp] = set()
        ts_mac_map[ref_computer.timestamp].add(ref_computer.ClientMacAddr)

    common = set()
    for key, value in ts_mac_map.items():
        if len(common) == 0:
            common = value
        else:
            common = common.intersection(value)
    return list(common)


def find_matches(query_mac_address, max_distance_feet, contact_time_min):
    if query_mac_address not in mac_map.keys():
        print(query_mac_address + ' not found.')
        return []

    query_mac_positions = mac_map[query_mac_address]
    ans = []
    for query_mac_position in query_mac_positions:
        ans += find_match(query_mac_position, max_distance_feet, contact_time_min)
    return set(ans)


df = dd.read_csv('firstfloor.csv')
df = df.compute()
full_df_date = (df['localdate'] >= '2020-04-05') & (df['localdate']<='2020-04-05')
df = df.loc[full_df_date]
computers = []
mac_map = {}
ind = 0
for index, row in df.iterrows():
    c = Computer(row['ClientMacAddr'], row['lat'], row['lng'], row['localtime'])
    if c.ClientMacAddr not in mac_map.keys():
        mac_map[c.ClientMacAddr] = []
    computers.append(c)
    mac_map[c.ClientMacAddr].append(ind)
    ind += 1

## query = []
## query = mac_map.keys()
#query = ['9c:8c:6e:46:1c:5e']
#for ind in query:
  #  ans = find_matches(ind, max_distance_feet=10, contact_time_min=5)
  #  if len(ans) == 0:
  #      print('No positive for: ' + ind)
  #  else:
  #      print('Positive matches for: ' + ind)
  #      print(ans)
 #   print()

def covid_contact_tracing(query_id, date_begin, date_end, distance_in_feet, time_in_contact):

    total_contact = set()
    df = dd.read_csv('firstfloor.csv')
    df = df.compute()
    full_df_date = (df['localdate'] >= date_begin) & (df['localdate'] <= date_end)
    df = df.loc[full_df_date]
    computers = []
    mac_map = {}
    ind = 0
    for index, row in df.iterrows():
        c = Computer(row['ClientMacAddr'], row['lat'], row['lng'], row['localtime'])
        if c.ClientMacAddr not in mac_map.keys():
            mac_map[c.ClientMacAddr] = []
        computers.append(c)
        mac_map[c.ClientMacAddr].append(ind)
        ind += 1
    output = find_matches(query_id, max_distance_feet=distance_in_feet, contact_time_min=time_in_contact)
    total_contact = total_contact.union(output)

    df = dd.read_csv('secondfloor.csv')
    df = df.compute()
    full_df_date = (df['localdate'] >= date_begin) & (df['localdate'] <= date_end)
    df = df.loc[full_df_date]
    computers = []
    mac_map = {}
    ind = 0
    for index, row in df.iterrows():
        c = Computer(row['ClientMacAddr'], row['lat'], row['lng'], row['localtime'])
        if c.ClientMacAddr not in mac_map.keys():
            mac_map[c.ClientMacAddr] = []
        computers.append(c)
        mac_map[c.ClientMacAddr].append(ind)
        ind += 1
    output = find_matches(query_id, max_distance_feet=distance_in_feet, contact_time_min=time_in_contact)
    total_contact = total_contact.union(output)

    df = dd.read_csv('thirdfloor.csv')
    df = df.compute()
    full_df_date = (df['localdate'] >= date_begin) & (df['localdate'] <= date_end)
    df = df.loc[full_df_date]
    computers = []
    mac_map = {}
    ind = 0
    for index, row in df.iterrows():
        c = Computer(row['ClientMacAddr'], row['lat'], row['lng'], row['localtime'])
        if c.ClientMacAddr not in mac_map.keys():
            mac_map[c.ClientMacAddr] = []
        computers.append(c)
        mac_map[c.ClientMacAddr].append(ind)
        ind += 1
    output = find_matches(query_id, max_distance_feet=distance_in_feet, contact_time_min=time_in_contact)
    total_contact = total_contact.union(output)

    df = dd.read_csv('groundfloor.csv')
    df = df.compute()
    full_df_date = (df['localdate'] >= date_begin) & (df['localdate'] <= date_end)
    df = df.loc[full_df_date]
    computers = []
    mac_map = {}
    ind = 0
    for index, row in df.iterrows():
        c = Computer(row['ClientMacAddr'], row['lat'], row['lng'], row['localtime'])
        if c.ClientMacAddr not in mac_map.keys():
            mac_map[c.ClientMacAddr] = []
        computers.append(c)
        mac_map[c.ClientMacAddr].append(ind)
        ind += 1
    output = find_matches(query_id, max_distance_feet=distance_in_feet, contact_time_min=time_in_contact)
    total_contact = total_contact.union(output)
    return(total_contact)

def covid_existance(mac_id_input):
    covid_df = pd.read_csv('example_with_covid.tsv', sep='\t')
    if mac_id_input not in pd.unique(covid_df['ClientMacAddr']):
        return ('There is no warning to take a Covid test, stay safe!')
    if mac_id_input in pd.unique(list(covid_df['ClientMacAddr'])):
        cluster_num = covid_df[covid_df['ClientMacAddr'] == mac_id_input]['cluster'].unique()[0]
        target_df = covid_df[covid_df['cluster'] == cluster_num]
        if 1 in pd.unique(target_df['covid_test']):
            return ('Since one of the member in your group had a positive result in Covid test.\nWe highly recommend you to schedule a Covid test.')
        else:
            return ( 'There is no warning to take a Covid test, stay safe!')
###
def contact_tracing_cluster(mac_id_input):
    covid_df = pd.read_csv('example_with_covid.tsv', sep='\t')
    if mac_id_input in pd.unique(list(covid_df['ClientMacAddr'])):
        cluster_num = list(covid_df[covid_df['ClientMacAddr'] == mac_id_input]['cluster'].unique())
        result = []
        for x in cluster_num:
            mac_id_result = covid_df[covid_df['cluster'] == x]['ClientMacAddr']
            result.append(mac_id_result)
        return list(result[0][1:])

# Create a page dropdown
page = st.selectbox("Choose Tool", ["Contact Tracing Tool", "Self Check"])
if page == "Contact Tracing Tool":
    st.write("""
    ## USC Data Explorer Contact Tracing Tool

    Please fill **Mac ID**, **begin date**, **end date**, **screen distance** and **contact time** below.
    """)

    # Fill boxes
    mac_id = st.text_input("MAC id", type="default", value='9c:8c:6e:46:1c:5e')
    begin_date = st.text_input("Begin Date", '2020-04-05')
    end_date = st.text_input("End Date", '2020-04-05')
    screen_distance = st.number_input("Screen Distance(ft)", min_value=0, max_value=100, value=30)
    contact_time = st.number_input("Contact Time(min)", min_value=0, max_value=200, value=1)


    if st.button('Search'):
        st.write('Close Contact:')
        st.text(covid_contact_tracing(mac_id, begin_date, end_date, screen_distance, contact_time))
        st.write('Same Cluster Member:')
        st.text(contact_tracing_cluster(mac_id))
elif page == "Self Check":
    st.write("""
    ## USC Data Explorer Self Checking Tool

    Please fill **Mac ID** below.
    """)
    mac_id_fill = st.text_input("MAC id", type="default", value='9c:8c:6e:46:1c:5e')
    if st.button('check'):
        st.text(covid_existance(mac_id_fill))

