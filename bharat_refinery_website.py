#Import Modules
import re
import pickle
import requests
import yaml
import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st
from textblob import TextBlob
import tableauserverclient as TSC
from yaml.loader import SafeLoader
from geopy.geocoders import Nominatim
from cryptography.fernet import Fernet
from streamlit_lottie import st_lottie
import streamlit_authenticator as stauth
from streamlit_option_menu import option_menu
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import avg, sum, col,lit

#------------------------------------------Reading Credentials------------------------------------
def get_credentials():
    key_file = 'key.key'
    key = ''

    with open('key.key','r') as key_in:
        key = key_in.read().encode()

    f = Fernet(key)

    fp = open("CredFile","rb")
    data = pickle.load(fp)
    username = data["user_name"].strip()
    password = f.decrypt(data['password'].encode()).decode().strip()
    account = data["account"].strip()
    fp.close()
    return username, password, account

#------------------------------------------Snowflake Python Connection----------------------------
# Create Session object
def create_session_object():
    username, password, account = get_credentials()
    connection_parameters = {
        "account": account,
        "user": username,
        "password": password,
        "role": "DATA_ENGINEER_ROLE",
        "warehouse": "DEV_WH",
        "database": "DEV_DB",
        "schema": "LANDING_SCHEMA"
        }
    session = Session.builder.configs(connection_parameters).create()
    return session 

#Keeping Connection Session Variable in Session-State
if 'session' not in st.session_state:
	st.session_state.session = create_session_object()
if 'authentication_status' not in st.session_state:
    st.session_state.authentication_status = False
#Page Layout
st.set_page_config(
    page_title="Bharat Refinery",
    page_icon=":fuelpump:",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://developers.snowflake.com',
        'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit"
        }
    )

#------------------Fetching GIF-----------------------------------------------
@st.cache(ttl=1200)
def lottie_url(url):
    r = requests.get(url)
    if r.status_code != 200:
        return None
    return r.json()

refinery = lottie_url('https://assets1.lottiefiles.com/private_files/lf30_mnZRTk.json')
filling_station = lottie_url('https://assets9.lottiefiles.com/packages/lf20_zqojwy4n.json')

#Sidebar of Webpage
with st.sidebar:
    if st.session_state.authentication_status:
        if(st.session_state.username.startswith('bhwh')):
           choose = option_menu("Welcome", ["About Us", "Filling Station", "Feedback", "Dashboard"],
                            icons=['house', 'geo-alt', 'chat left text', 'file-bar-graph'],
                            menu_icon="app-indicator", default_index=0,
                            styles={
                "container": {"padding": "5!important", "background-color": "#586e75"},
                "icon": {"color": "#002b36", "font-size": "23px"}, 
                "nav-link": {"font-size": "16px", "text-align": "left", "margin":"0px", "--hover-color": "#586e75"},
                "nav-link-selected": {"background-color": "#189AB4"},
            }
            )
        else: 
            choose = option_menu("Welcome", ["About Us", "Filling Station", "Optimal Path", "Dashboard"],
                                icons=['house', 'geo-alt', 'map', 'file-bar-graph'],
                                menu_icon="app-indicator", default_index=0,
                                styles={
                "container": {"padding": "5!important", "background-color": "#586e75"},
                "icon": {"color": "#002b36", "font-size": "23px"}, 
                "nav-link": {"font-size": "16px", "text-align": "left", "margin":"0px", "--hover-color": "#586e75"},
                "nav-link-selected": {"background-color": "#189AB4"},
            }
            )
    else:
        choose = option_menu("Welcome", ["About Us", "Filling Station"],
                            icons=['house', 'geo-alt'],
                            menu_icon="app-indicator", default_index=0,
                            styles={
            "container": {"padding": "5!important", "background-color": "#586e75"},
            "icon": {"color": "#002b36", "font-size": "23px"}, 
            "nav-link": {"font-size": "16px", "text-align": "left", "margin":"0px", "--hover-color": "#586e75"},
            "nav-link-selected": {"background-color": "#189AB4"},
        }
        )
    
    #---------------LOGIN---------------------------------
    with open('config.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)

    authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
        config['preauthorized']
    )

    name, authentication_status, username = authenticator.login('Admin Login', 'main')

    if authentication_status == False:
        st.error("Username/password is incorrect")

    if authentication_status == None:
        st.warning("Please enter your username and password")

    if authentication_status:
        temp = authenticator.logout("Logout", "sidebar")
        st.sidebar.title(f"Welcome {name} Team")

#------------------------------------------Header-------------------------------------------
st.markdown(""" <style> .font_header {
        font-size:40px ; font-family: 'Cooper Black'; color: #fafafa;} 
        </style> """, unsafe_allow_html=True)
st.markdown('<p class="font_header" align="center">Bharat Refinery</p>', unsafe_allow_html=True)

#-------------------------------------------Common CSS Parameters----------------------------
st.markdown(""" <style> .font_other {
    font-size:30px ; font-family: 'Cooper Black'; color: #fafafa;} 
    </style> """, unsafe_allow_html=True)

#------------------ABOUT-----------------------
if choose == "About Us":
    col1, col2 = st.columns([1,2])
    with col1:
        st_lottie(refinery, height=300,width=300)
    with col2:
        st.write('##')
        st.write("**Welcome to Bharat Refinery.**")
        st.write("""➦ We are among the leading oil and gas companies in India.""")
        st.write("""➦ The organisation aims to provide only the purest oil across India.""")
        st.write("""➦ We are committed to offer customer friendly Fuel Stations across the country.""")
        st.write("""➦ We are the proud owner of two refinery units spread across Mumbai, Kochi.""")
        st.write('---')
    
#------------------Filling Station-----------------------
elif choose == "Filling Station":
    st.markdown("#### Filling Station :fuelpump:")
    col1, col2 = st.columns([2,1])
    with col1:
        #st.write("*Enter Pincode to Get the Charging Stations!")
        location = st.text_input("", placeholder="Enter Pincode or City Here!")
        if location:
            clicked = st.button("Search", help="click to get filling stations")
            
            if clicked:
                #concatinating all the tools & technology
                df = st.session_state.session.table("DEV_DB.LANDING_SCHEMA.PETROL_PUMP_DATA").to_pandas()
                df.fillna('', inplace=True)
                df["Others"] = df["PINCODE"].str.cat(df[["CITY"]].astype(str), sep=",")
                location = [location]
                df = df[df['Others'].str.contains('|'.join(location), flags=re.IGNORECASE)]
                if(df.empty==True):
                    st.error("No Filling Station Found in this Pincode!")
                else:
                    data_types_dict = {'LATITUDE': float, 'LONGITUDE': float}
                    df = df.astype(data_types_dict)
                    
                    view_state = pdk.ViewState(latitude=df['LATITUDE'].max(), longitude=df['LONGITUDE'].min(), zoom=10, pitch=1)
                    map_data = df[["LATITUDE", "LONGITUDE"]]

                    tooltip = {
                        "html":
                        "<b>Station Name: </b>{CITY}_{WAREHOUSE_ID} <br/>"
                        "<b>Pincode:</b> {PINCODE} <br/>"
                        "<b>City:</b> {CITY} <br/>",
                        "style": {
                            "backgroundColor": "steelblue",
                            "color": "black",
                            }
                        }
                    
                    slayer = pdk.Layer(
                        type='ScatterplotLayer',
                        data=df,
                        get_position=["LONGITUDE", "LATITUDE"],
                        get_color=["30", "144", "237"],
                        get_line_color=[0, 0, 0],
                        get_radius=550,
                        pickable=True,
                        onClick=True,
                        filled=True,
                        line_width_min_pixels=10,
                        opacity=2,
                    )
                
                    layert1 = pdk.Layer(
                        type="TextLayer",
                        data=df,
                        pickable=False,
                        get_position=["LONGITUDE", "LATITUDE"],
                        get_text="name",
                        get_size=3000,
                        sizeUnits='meters',
                        get_color=[0, 0, 0],
                        get_angle=0,
                        # Note that string constants in pydeck are explicitly passed as strings
                        # This distinguishes them from columns in a data set
                        getTextAnchor= '"middle"',
                        get_alignment_baseline='"bottom"'
                    )

                    pp = pdk.Deck(
                        initial_view_state=view_state,
                        map_provider='mapbox',
                        map_style=pdk.map_styles.LIGHT,
                        layers=[
                            slayer,
                            layert1
                            ],
                        tooltip=tooltip
                    )
                    
                    deckchart = st.pydeck_chart(pp)
                
    with col2:
        st_lottie(filling_station, height=0,width=0)


#------------------CVPR-----------------------
elif choose == "Optimal Path":
    st.markdown("#### Find Optimal Path :truck:")

    #-----------------Optimal Path ------------------
    def find_optimal_path(wh):
        wh = wh+'.json'
        df = pd.read_json('./data/'+wh)
        def hex_to_rgb(h):
            h = h.lstrip('#')
            return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
            
        df['color'] = df['color'].apply(hex_to_rgb)
        
        # initialize Nominatim API
        geolocator = Nominatim(user_agent="geoapiExercises")
        
        temp_dict = []
        for i in range(df["path"].count()):
            for j in range(len(df["path"][i])):
                d = {'LONGITUDE':df["path"][i][j][0] , 'LATITUDE':df["path"][i][j][1], 'DEMAND': df["demand"][i][j]}
                #, 'LOCATION':geolocator.reverse(str(df["path"][i][j][1])+","+str(df["path"][i][j][0]))
                if d not in temp_dict:
                    temp_dict.append(d)
        df2 = pd.DataFrame(temp_dict)

        location = []
        for i in range(len(df2)):
            location.append(str(geolocator.reverse(str(df2.iloc[i,1])+","+str(df2.iloc[i,0]))))
        df2["LOCATION"] = location
        
        view_state = pdk.ViewState(
            latitude=df['path'].max()[0][1],
            longitude=df['path'].max()[0][0],
            zoom=10, 
            pitch=1
        )

        tooltip = {
                "html":
                "<b>Location: </b>{LOCATION} <br/>"
                "<b>Req Qty: </b> {DEMAND}k Liter <br/>",
                "style": {
                    "backgroundColor": "steelblue",
                    "color": "black",
                    }
                }
        
        layer = pdk.Layer(
            type='PathLayer',
            data=df,
            pickable=True,
            get_color='color',
            width_scale=2,
            width_min_pixels=2,
            get_path='path',
            get_width=5
        )
        
        slayer = pdk.Layer(
            type='ScatterplotLayer',
            data=df2,
            get_position=["LONGITUDE", "LATITUDE"],
            get_color=["30", "144", "237"],
            get_line_color=[0, 0, 0],
            get_radius=200,
            pickable=True,
            onClick=True,
            filled=True,
            line_width_min_pixels=10,
            opacity=2,
        )
        
        r = pdk.Deck(layers=[layer, slayer], initial_view_state=view_state, tooltip=tooltip)
        st.pydeck_chart(r)
    
    wh = st.text_input("", placeholder="Enter Warehouse ID!")
    clicked = st.button("Search", help="click to get optimal path")
    if clicked:
        find_optimal_path(wh)

#-------------------------------------------FEEDBACK----------------------------------------
elif choose == "Feedback":
    st.markdown("#### Feedback :speaker:")
    with st.form("feedback_form"):
        st.write('<style>div.row-widget.stRadio > div{flex-direction:row;}</style>', unsafe_allow_html=True)
        delivery_rating = st.radio("Timely Delivery : ",("1","2","3","4","5"))
        quality_rating  = st.radio("Fuel Quality : ",("1","2","3","4","5"))
        quantity_rating  = st.radio("Requested Quntity Supplied?",("Yes","No"))
        other = st.text_area("Anything else?")

        if st.form_submit_button("Submit"):
                #Insert Data to Snowflake
                if(other=='' or other.isspace()):
                    other = "All Good"
                
                #Analysing  Sentiment
                analysis = TextBlob(other)
                polarity_text = analysis.sentiment.polarity
                subjectivity_text = analysis.sentiment.subjectivity
                if polarity_text > 0:
                    sentiment='Positive'
                elif polarity_text == 0:
                    sentiment='Neutral'
                else:
                    sentiment='Negative'

                #writing data to snowflake
                df = pd.DataFrame({"warehouse":[st.session_state.username], "delivery_rating":[int(delivery_rating)], "quality_rating":[int(quality_rating)], "quantity_rating":[quantity_rating], "other_feedback":[other], "sentiment":[sentiment], "polarity":[polarity_text], "subjectivity":[subjectivity_text] })
                python_df = st.session_state.session.create_dataframe(df)
                python_df.write.mode("append").save_as_table("feedback")

                st.success("Thanks for Your Feedback!")


#-------------------------------------------FEEDBACK----------------------------------------
elif choose == "Dashboard":
    st.markdown("#### Dashboard 📊")

    tableau_auth = TSC.PersonalAccessTokenAuth(
        st.secrets["tableau"]["token_name"],
        st.secrets["tableau"]["personal_access_token"],
        st.secrets["tableau"]["site_id"],
    )
    server = TSC.Server(st.secrets["tableau"]["server_url"], use_server_version=True)

    # Get various data.
    # Explore the tableauserverclient library for more options.
    # Uses st.experimental_memo to only rerun when the query changes or after 10 min.
    @st.experimental_memo(ttl=1200)
    def run_query(view_name):
        with server.auth.sign_in(tableau_auth):

            # Get our workbook.
            workbooks, pagination_item = server.workbooks.get()
            for w in workbooks:
                if w.name == 'BHARAT_REFINERY_DASHBOARD_FINAL':
                    our_workbook = w
                    break

            # Get views for BHARAT_REFINERY_DASHBOARD_FINAL workbook.
            server.workbooks.populate_views(our_workbook)
            for v in our_workbook.views:
                if view_name == v.name:
                    our_view = v
                    break
            print(our_view.name)

            # Get image for first view of first workbook.
            server.views.populate_image(our_view)
            view_image = our_view.image

            return view_image

        run_query(view_name)

    if(st.session_state.username=='operation'):
        
        view_image = run_query('1_Demand_&_Supply')
        st.image(view_image, width=800)

        view_image = run_query('3_Wastage_&_Loss Control')
        st.image(view_image, width=800)

        view_image = run_query('4_Warehouse_Operation_Dashboard')
        st.image(view_image, width=800)

    if(st.session_state.username=='logistic'):
        view_image = run_query('2_Transport')
        st.image(view_image, width=800)
    
    if(st.session_state.username=='bhwh01'):
        view_image = run_query('4_Warehouse_Operation_Dashboard')
        st.image(view_image, width=800)
    

