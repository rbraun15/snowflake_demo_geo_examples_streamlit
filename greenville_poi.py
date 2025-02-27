# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import folium
import shapely
import geopandas as gpd
import json5
import streamlit_folium as stf
import h3
from shapely import wkb
import altair as alt
import pydeck as pdk


# Write directly to the app
st.title("Places of Interest in Greenville :earth_americas:")

st.write(
    """Streamlit App built on Snowflake to visualize **Greenville County Addresses**.     
      For more information on Greenville County SC, please visit
    [Greenville County](https://www.latlong.net/place/greenville-sc-usa-10040.html).
    """)

# Get the current credentials
session = get_active_session()



# -------------------------------------------------------------------------------------
# Sidebar filter for Restaurant Type
# -------------------------------------------------------------------------------------
category_query = "select distinct category_main from demo_map.addresses.ADDRESSES_TO_MAP where category_main not in ('Places_Lived','Places_Worked');"
category_df = session.sql(category_query).to_pandas()
selected_category_type = st.sidebar.multiselect("Select Restaurant Type", category_df)

# Sidebar filter for the number of stations
num_stations = st.sidebar.number_input("No of restaurants to show:", min_value=10, max_value=100, step=10, value=10)

# mapbox API key
mbkey = ''


 
# -------------------------------------------------------------------------------------
# Places I have lived  
# -------------------------------------------------------------------------------------
placeslived_query = f"""select POI_NAME,CATEGORY_MAIN, ZIP, LONGITUDE, LATITUDE  from demo_map.addresses.ADDRESSES_TO_MAP where CATEGORY_MAIN='Places_Lived'"""
plived_df = session.sql(placeslived_query).to_pandas()
plived_df_col = session.sql(placeslived_query).collect()

plived_layer = pdk.Layer(
    type='ScatterplotLayer', 
    data=plived_df, 
    pickable=True,
    get_position='[LONGITUDE, LATITUDE]', 
    get_color='[215,21,99]',
    # get_color='[128,0,128,200]',   
    get_radius=300,
    opacity=0.1,
    auto_highlight=True,
    id = 'plived_layer_id' 
)

# Above options explained
# type = type of layer - "ScatterplotLayer", "HexagonLayer", "GeoJsonLayer", "LineLayer", "PathLayer", etc.
# data = the pandas data frame, can be GeoJSON objet or a list
# get_radius = size of the point on the map
# get_position = location, this exmaple is based on query result
# get_fill_color - color of areas like polygons or hexagons
# get_color = color of each data point, a column called "color" with RGB values or any color scale logic. https://www.rapidtables.com/web/color/RGB_Color.html
# opacity = value 0 - 1, 0 = totally transparent, 1 = completely opaque
# pickable = allows interafction, if not set to true the would not see tooltip
# auto_highlight = set to True and as you hover over a point it gets highlighted, 
#   seems useful to make it clear which point you are looking at


# -------------------------------------------------------------------------------------
# Places I have worked  
# -------------------------------------------------------------------------------------
placesworked_query = f"""select POI_NAME,CATEGORY_MAIN, ZIP, LONGITUDE, LATITUDE from demo_map.addresses.ADDRESSES_TO_MAP where CATEGORY_MAIN='Places_Worked'"""
pworked_df = session.sql(placesworked_query).to_pandas()
pworked_df_col = session.sql(placesworked_query).collect()

pworked_layer = pdk.Layer(
    type='ScatterplotLayer', 
    data=pworked_df, 
    pickable=True,
    get_position='[LONGITUDE, LATITUDE]', 
    get_color='[50,150,50,200]', 
    get_radius=150, 
    id = 'pworked_layer_id'
)


# -------------------------------------------------------------------------------------
# Restaurants 
# -------------------------------------------------------------------------------------
restaurant_query = f"""select POI_NAME,CATEGORY_MAIN, ZIP, LONGITUDE, LATITUDE from demo_map.addresses.ADDRESSES_TO_MAP where category_main not in ('Places_Lived','Places_Worked')"""
prestaurant_df = session.sql(restaurant_query).to_pandas()
prestaurant_df_col = session.sql(restaurant_query).collect()

filtered_prestaurant_df  = prestaurant_df[prestaurant_df['CATEGORY_MAIN'].isin(selected_category_type)] if selected_category_type else prestaurant_df

restaurant_layer = pdk.Layer(
    type='ScatterplotLayer', 
    #data=prestaurant_df, 
    data=filtered_prestaurant_df, 
    pickable=True,
    get_position='[LONGITUDE, LATITUDE]', 
    get_color='[0,0,255]', 
    get_radius=150, 
    id = 'restaurant_layer_id'
                      )
# -------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------
# create a tooltip for the plot layer
# -------------------------------------------------------------------------------------
# Not clear where the tool tip db data is coming from, the last query??
# Appears tooltip fields come from the query immediately above
tooltip = {
        "html": 
                "<b>Name:</b> {POI_NAME} <br/>"
                "<b>Category Type:</b> {CATEGORY_MAIN} <br/>"
                "<b>ZIP:</b> {ZIP}",
            "style": {
                    "backgroundColor": 'rgba(128, 0, 128, 0.5)',
                    "color": "white"}}
# -------------------------------------------------------------------------------------

# -------------------------------------------------------------------------------------
# Path Example 
# -------------------------------------------------------------------------------------


path_query = f"""select POI_NAME,CATEGORY_MAIN, ZIP, LONGITUDE, LATITUDE from demo_map.addresses.ADDRESSES_TO_MAP where CATEGORY_MAIN='Places_Worked'"""
path_df = session.sql(path_query).to_pandas()
path_df_col = session.sql(path_query).collect()

# display table of data to map
# dev
# st.write(path_df)

def prepare_path_data(path_df):
        return path_df[['LONGITUDE', 'LATITUDE']].values.tolist()

path_data = prepare_path_data(path_df)


# Create a PathLayer in pydeck, all points must be in one row

path_layer = pdk.Layer(
    type="PathLayer",
    data=[{'coordinates': path_data}],  # Wrap path_data in a dict with a 'coordinates' key
    get_path="coordinates",  # Ensure it refers to the 'coordinates' field in the data
    get_color=[255, 0, 0],  # Red color for the path
    width_scale=20,
    width_min_pixels=2,
)



# -------------------------------------------------------------------------------------
# Define and display the map
# -------------------------------------------------------------------------------------

# define map details - layers, map provider, initial state, tooltip
deck_all_layers = pdk.Deck(
        map_provider = 'mapbox',
        api_keys = {"mapbox":mbkey},
        map_style = 'mapbox://styles/mapbox/light-v11',
        layers=[
             pworked_layer,
             plived_layer,
             restaurant_layer,
             path_layer
               ], 
        tooltip = tooltip,
        initial_view_state=pdk.ViewState(
        latitude=34.852619,
        longitude=-82.394012,
        zoom=10,
        pitch=30
        )
        )
st.write("Points and Path Example.")
# display the map
st.pydeck_chart(deck_all_layers)



# -------------------------------------------------------------------------------------
# Display a table with filtered records
# -------------------------------------------------------------------------------------
allpoints_query = "select * from demo_map.addresses.ADDRESSES_TO_MAP"
allpoints_df = session.sql(allpoints_query).to_pandas()
allpoints_filtered_df = allpoints_df[allpoints_df['CATEGORY_MAIN'].isin(selected_category_type)] if selected_category_type else allpoints_df
st.subheader("Points of Interest in Greenville")

st.write("All Points")
st.write(allpoints_df)

st.write("Filtered Points")
st.write(allpoints_filtered_df)
#st.table(filtered_tqdf2.style.set_properties(**{'max-height': '400px', 'overflow-y': 'auto'}))


