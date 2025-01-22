import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime


# Streamlit app
st.set_page_config(
    page_title="US Ski & Snowboard Ski Town Race - www.skitownrace.com", 
    page_icon="❄️", 
    layout="wide"
    )

st.markdown("""
<style>
    [data-testid=stSidebar] {
        background-color: #0F1D41;
        color: #ffffff
    }
    [data-testid=stSelectbox] [data-testid=stWidgetLabel],
      {
        font-size: large;
        color: #ffffff;
    }
            
    [data-testid=stWrite] {
        color: #ffffff;
        line-height: 10px;
    }
            
    [data-testid=stForm] {
        border: 3px #DA262E solid;
        background-color: rgba(259, 250, 250, 0.9);
    }
            
    button div p {
        color: #0F1D41;
    }
      
</style>
""", unsafe_allow_html=True)













# Initialize session state variables
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user_email' not in st.session_state:
    st.session_state.user_email = None

# Snowflake connection setup
@st.cache_resource
def create_connection():
    return snowflake.connector.connect(
        user = st.secrets["SNOWFLAKE_USERNAME"],
        password = st.secrets["SNOWFLAKE_PASSWORD"],
        account = st.secrets["SNOWFLAKE_ACCOUNT"],
        role = st.secrets["SNOWFLAKE_ROLE"],
        warehouse = st.secrets["SNOWFLAKE_WAREHOUSE"],
        database = st.secrets["SNOWFLAKE_DATABASE"],
        schema = st.secrets["SNOWFLAKE_SCHEMA"],
    )

def create_registration_table(conn):
    with conn.cursor() as cur:
        # 1. Create REGISTRATIONS table first
        cur.execute("""
        CREATE TABLE IF NOT EXISTS REGISTRATIONS (
            UID INTEGER IDENTITY(1,1) PRIMARY KEY,
            MEMBER_ID INTEGER DEFAULT 0,
            US_ID VARCHAR,               -- Optional
            FIS_ID VARCHAR,              -- Optional
            EMAIL VARCHAR NOT NULL UNIQUE,
            PASSWORD VARCHAR NOT NULL,
            FIRST_NAME VARCHAR NOT NULL,
            LAST_NAME VARCHAR NOT NULL,
            FULL_NAME VARCHAR NOT NULL,
            DOB DATE NOT NULL,
            DIVISION VARCHAR NOT NULL,
            TEAM VARCHAR,                -- Optional
            DISCIPLINE VARCHAR           -- Optional
        )
        """)

        # 2. Create UPCOMING_EVENTS table with CREATOR_ID defined in the column list
        cur.execute("""
        CREATE TABLE IF NOT EXISTS UPCOMING_EVENTS (
            EVENT_ID INTEGER IDENTITY(1,1) PRIMARY KEY,
            EVENT_NAME VARCHAR NOT NULL,
            EVENT_DATE DATE NOT NULL,
            COMPETITOR_COUNT INTEGER DEFAULT 0,
            LOCATION VARCHAR,
            CITY VARCHAR NOT NULL,
            STATE VARCHAR(2) NOT NULL,
            ZIP VARCHAR(10) NOT NULL,
            VENUE VARCHAR NOT NULL,
            DIVISION VARCHAR NOT NULL,
            DISCIPLINE VARCHAR NOT NULL,
            CREATOR_ID INTEGER,          -- Added CREATOR_ID here
            FEE DECIMAL(10,2),
            URL VARCHAR,
            FOREIGN KEY (CREATOR_ID) REFERENCES REGISTRATIONS(UID)
        )
        """)

        # 3. Create EVENT_REGISTRATIONS table last
        cur.execute("""
        CREATE TABLE IF NOT EXISTS EVENT_REGISTRATIONS (
            REGISTRATION_ID INTEGER IDENTITY(1,1) PRIMARY KEY,
            EVENT_ID INTEGER NOT NULL,
            USER_ID INTEGER NOT NULL,
            BIB_NUMBER INTEGER,
            REGISTRATION_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            FOREIGN KEY (EVENT_ID) REFERENCES UPCOMING_EVENTS(EVENT_ID),
            FOREIGN KEY (USER_ID) REFERENCES REGISTRATIONS(UID),
            UNIQUE (EVENT_ID, USER_ID),
            UNIQUE (EVENT_ID, BIB_NUMBER)
        )
        """)

        # Create chat history table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS CHAT_HISTORY (
            MESSAGE_ID INTEGER IDENTITY(1,1) PRIMARY KEY,
            USER_ID INTEGER,
            MESSAGE_TEXT TEXT NOT NULL,
            IS_BOT BOOLEAN DEFAULT FALSE,
            TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            FOREIGN KEY (USER_ID) REFERENCES REGISTRATIONS(UID)
        )
        """)
        
        conn.commit()




def register_user(conn, us_id, fis_id, email, password, first_name, last_name, dob, division, team, discipline):
    try:
        # Set empty optional fields to None
        us_id = us_id if us_id.strip() else None
        fis_id = fis_id if fis_id.strip() else None
        team = team if team.strip() else None
        discipline_str = ", ".join(discipline) if discipline else None
        
        full_name = f"{first_name} {last_name}"
        
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO REGISTRATIONS 
                (US_ID, FIS_ID, EMAIL, PASSWORD, FIRST_NAME, LAST_NAME, FULL_NAME, 
                 DOB, DIVISION, TEAM, DISCIPLINE)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (us_id, fis_id, email, password, first_name, last_name, full_name,
                 dob, division, team, discipline_str)
            )
            conn.commit()
            
            # Get the assigned MEMBER_ID
            cur.execute("""
                SELECT MEMBER_ID 
                FROM REGISTRATIONS 
                WHERE EMAIL = %s
            """, (email,))
            
            member_id = cur.fetchone()[0]
            st.success(f"Registration successful! Your Member ID is: {member_id}")
            return True
            
    except Exception as e:
        st.error(f"Error: {e}")
        if conn:
            conn.rollback()
        return False
            
def verify_login(conn, email, password):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EMAIL, FIRST_NAME, LAST_NAME 
                FROM REGISTRATIONS 
                WHERE EMAIL = %s AND PASSWORD = %s
                """,
                (email, password)
            )
            result = cur.fetchone()
            if result:
                full_name = f"{result[1]} {result[2]}"
                return True, full_name
            return False, None
    except Exception as e:
        st.error(f"Login error: {e}")
        return False, None

def get_user_info(conn, email):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT 
            MEMBER_ID, 
            EMAIL, 
            FIRST_NAME,
            LAST_NAME,
            DOB,
            TEAM,
            DIVISION, 
            DISCIPLINE
        FROM REGISTRATIONS
        WHERE EMAIL = %s
        """, (email,))
        row = cur.fetchone()
        if row:
            return {
                'Member ID': row[0],
                'Email': row[1],
                'Name': f"{row[2]} {row[3]}",
                'DOB': row[4],
                'Team': row[5],
                'Division': row[6],
                'Discipline': row[7]
            }
        return None

def get_user_id(conn, email):
    with conn.cursor() as cur:
        cur.execute("SELECT UID FROM REGISTRATIONS WHERE EMAIL = %s", (email,))
        result = cur.fetchone()
        return result[0] if result else None

def add_event(conn, event_name, event_date, location, city, state, zip_code, venue, discipline, division, creator_id, fee=None, url=None):
    """
    Add a new event to the database with proper error handling and parameter validation.
    """
    try:
        # Validate required fields
        required_fields = {
            'event_name': event_name,
            'event_date': event_date,
            'city': city,
            'state': state,
            'zip_code': zip_code,
            'venue': venue,
            'discipline': discipline,
            'division': division,
            'creator_id': creator_id
        }
        
        # Check for missing required fields
        missing_fields = [field for field, value in required_fields.items() if not value]
        if missing_fields:
            st.error(f"Missing required fields: {', '.join(missing_fields)}")
            return False
            
        with conn.cursor() as cur:
            sql = """
            INSERT INTO UPCOMING_EVENTS (
                EVENT_NAME,
                EVENT_DATE,
                LOCATION,
                CITY,
                STATE,
                ZIP,
                VENUE,
                DISCIPLINE,
                DIVISION,
                CREATOR_ID,
                FEE,
                URL
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            values = (
                event_name,
                event_date,
                location,
                city,
                state,
                zip_code,
                venue,
                discipline,
                division,
                creator_id,
                fee,
                url
            )
            
            cur.execute(sql, values)
            conn.commit()
            return True
            
    except Exception as e:
        st.error(f"Error in add_event: {str(e)}")
        if conn:
            conn.rollback()
        return False

def is_event_creator(conn, event_id, user_id):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT COUNT(*) 
        FROM UPCOMING_EVENTS 
        WHERE EVENT_ID = %s AND CREATOR_ID = %s
        """, (event_id, user_id))
        return cur.fetchone()[0] > 0

def edit_event(conn, event_id, event_name, event_date, location, city, state, zip_code, venue, discipline, division, fee, url):
    try:
        with conn.cursor() as cur:
            cur.execute("""
            UPDATE UPCOMING_EVENTS 
            SET EVENT_NAME = %s, EVENT_DATE = %s, LOCATION = %s, CITY = %s, 
                STATE = %s, ZIP = %s, VENUE = %s, DISCIPLINE = %s, DIVISION = %s, URL = %s
            WHERE EVENT_ID = %s
            """, (event_name, event_date, location, city, state, zip_code, venue, discipline, division, fee, url, event_id))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error updating event: {e}")
        return False

def delete_event(conn, event_id):
    try:
        with conn.cursor() as cur:
            # First delete any registrations for this event
            cur.execute("DELETE FROM EVENT_REGISTRATIONS WHERE EVENT_ID = %s", (event_id,))
            # Then delete the event
            cur.execute("DELETE FROM UPCOMING_EVENTS WHERE EVENT_ID = %s", (event_id,))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error deleting event: {e}")
        return False

def register_for_event(conn, event_id, user_id):
    try:
        with conn.cursor() as cur:
            # Get the next available bib number for this event
            cur.execute("""
            SELECT COALESCE(MAX(BIB_NUMBER), 0) + 1
            FROM EVENT_REGISTRATIONS
            WHERE EVENT_ID = %s
            """, (event_id,))
            next_bib = cur.fetchone()[0]
            
            # Insert the new registration with bib number
            cur.execute("""
            INSERT INTO EVENT_REGISTRATIONS (EVENT_ID, USER_ID, BIB_NUMBER)
            VALUES (%s, %s, %s)
            """, (event_id, user_id, next_bib))
            
            # Update competitor count
            cur.execute("""
            UPDATE UPCOMING_EVENTS 
            SET COMPETITOR_COUNT = (
                SELECT COUNT(*) 
                FROM EVENT_REGISTRATIONS 
                WHERE EVENT_ID = %s
            )
            WHERE EVENT_ID = %s
            """, (event_id, event_id))
            
            conn.commit()
            return True
    except Exception as e:
        if "duplicate key value violates unique constraint" in str(e).lower():
            st.error("You are already registered for this event.")
        else:
            st.error(f"Error registering for event: {e}")
        return False

def unregister_from_event(conn, event_id, user_id):
    try:
        with conn.cursor() as cur:
            # Remove registration
            cur.execute("""
            DELETE FROM EVENT_REGISTRATIONS 
            WHERE EVENT_ID = %s AND USER_ID = %s
            """, (event_id, user_id))
            
            # Update competitor count
            cur.execute("""
            UPDATE UPCOMING_EVENTS
            SET COMPETITOR_COUNT = (
                SELECT COUNT(*) 
                FROM EVENT_REGISTRATIONS 
                WHERE EVENT_ID = %s
            )
            WHERE EVENT_ID = %s
            """, (event_id, event_id))
            
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error unregistering from event: {e}")
        return False

def get_user_events(conn, user_id):
    try:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT 
                ue.EVENT_ID,
                ue.EVENT_NAME,
                ue.EVENT_DATE,
                ue.COMPETITOR_COUNT,
                ue.LOCATION,
                ue.CITY,
                ue.STATE,
                ue.ZIP,
                ue.VENUE,
                ue.DISCIPLINE,
                ue.URL,
                er.REGISTRATION_DATE,
                er.BIB_NUMBER
            FROM UPCOMING_EVENTS ue
            JOIN EVENT_REGISTRATIONS er ON ue.EVENT_ID = er.EVENT_ID
            WHERE er.USER_ID = %s
            ORDER BY ue.EVENT_DATE
            """, (user_id,))
            
            rows = cur.fetchall()
            if rows:
                return pd.DataFrame(rows, columns=[
                    'Event ID', 'Event Name', 'Event Date', 'Competitor Count', 
                    'Location', 'City', 'State', 'ZIP',
                    'Venue', 'Discipline', 'URL', 'Registration Date', 'Bib Number'
                ])
            return None
    except Exception as e:
        st.error(f"Error fetching user events: {e}")
        return None

def get_upcoming_events(conn, search_term=None, state_filter=None, discipline_filter=None):
    with conn.cursor() as cur:
        query = """
        SELECT 
            EVENT_ID,
            EVENT_NAME,
            EVENT_DATE,
            COMPETITOR_COUNT,
            LOCATION,
            CITY,
            STATE,
            ZIP,
            VENUE,
            DIVISION,
            DISCIPLINE,
            URL,
            CREATOR_ID
        FROM UPCOMING_EVENTS
        WHERE 1=1
        """
        params = []
        
        if search_term:
            query += """ AND (
                LOWER(EVENT_NAME) LIKE %s 
                OR LOWER(CITY) LIKE %s 
                OR LOWER(VENUE) LIKE %s
            )"""
            search_pattern = f"%{search_term.lower()}%"
            params.extend([search_pattern, search_pattern, search_pattern])
        
        if state_filter:
            query += " AND STATE = %s"
            params.append(state_filter)
            
        if discipline_filter:
            query += " AND DISCIPLINE = %s"
            params.append(discipline_filter)
            
        query += " ORDER BY EVENT_DATE"
        
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        if rows:
            return pd.DataFrame(rows, columns=[
                'Event ID', 'Event Name', 'Event Date', 'Competitor Count', 
                'Location', 'City', 'State', 'ZIP',
                'Venue', 'Division', 'Discipline', 'URL', 'Creator ID'
            ])
        return None

def is_admin(email):
    return email.endswith('@admin.com')

def logout():
    st.session_state.logged_in = False
    st.session_state.user_email = None
    st.rerun()

# Connect to Snowflake
conn = create_connection()
create_registration_table(conn)
















# PDF FUNCTIONS
def create_pdf_tables(conn):
    """Create tables for storing PDF content"""
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS PDF_DOCUMENTS (
            DOC_ID INTEGER IDENTITY(1,1),
            FILENAME VARCHAR NOT NULL,
            CONTENT TEXT,
            EMBEDDING VECTOR,
            TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """)
        conn.commit()

def process_pdf(conn, pdf_path):
    """Process a PDF and store its content"""
    try:
        import PyPDF2
        
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            text = ""
            for page in reader.pages:
                text += page.extract_text() + "\n"
                
            with conn.cursor() as cur:
                # Store the content
                cur.execute("""
                INSERT INTO PDF_DOCUMENTS (FILENAME, CONTENT)
                VALUES (%s, %s)
                """, (pdf_path.split('/')[-1], text))
                
                # Generate embedding using Cortex
                cur.execute("""
                UPDATE PDF_DOCUMENTS 
                SET EMBEDDING = CORTEX_EMBED_TEXT(CONTENT)
                WHERE EMBEDDING IS NULL
                """)
                
            conn.commit()
            return True
    except Exception as e:
        print(f"Error processing PDF: {str(e)}")
        return False

def get_relevant_content(conn, query):
    """Get relevant content from PDFs using semantic search"""
    with conn.cursor() as cur:
        cur.execute("""
        WITH query_embedding AS (
            SELECT CORTEX_EMBED_TEXT(?) as query_vector
        )
        SELECT 
            CONTENT,
            CORTEX_COSINE_SIMILARITY(EMBEDDING, query_vector) as similarity
        FROM PDF_DOCUMENTS
        CROSS JOIN query_embedding
        WHERE EMBEDDING IS NOT NULL
        ORDER BY similarity DESC
        LIMIT 3
        """, (query,))
        results = cur.fetchall()
        return [row[0] for row in results]

def get_enhanced_response(conn, query):
    """Get response using RAG"""
    try:
        # Get relevant content
        context = get_relevant_content(conn, query)
        if not context:
            return None
            
        # Format context
        context_text = "\n".join(context)
        
        # Generate response using Cortex
        with conn.cursor() as cur:
            cur.execute("""
            WITH PROMPT AS (
                SELECT ?||?||? as full_prompt
            )
            SELECT CORTEX_COMPLETION(full_prompt)
            FROM PROMPT
            """, (
                "Using the following context:\n\n",
                context_text,
                f"\n\nAnswer this question: {query}"
            ))
            result = cur.fetchone()
            return result[0] if result else None
            
    except Exception as e:
        print(f"Error getting enhanced response: {str(e)}")
        return None
    








# Configure Cortex Search with Mistral
def setup_cortex_search(session):
    # Create compute pool if it doesn't exist
    session.sql("""
        CREATE COMPUTE POOL IF NOT EXISTS mistral_pool
        MIN_NODES = 1
        MAX_NODES = 1
        INSTANCE_FAMILY = STANDARD_1
    """).collect()
    
    # Create service if it doesn't exist
    session.sql("""
        CREATE SERVICE IF NOT EXISTS mistral_service
        IN COMPUTE POOL mistral_pool
        MIN_INSTANCES = 1
        MAX_INSTANCES = 1
        SPEC = '{
            "type": "mistral",
            "model": "mistral-large-latest",
            "config": {
                "temperature": 0.7,
                "max_tokens": 1024
            }
        }'
    """).collect()



def setup_chat_tables(conn):
    """Create necessary tables for chat functionality"""
    with conn.cursor() as cur:
        # Create chat history table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS CHAT_HISTORY (
            MESSAGE_ID INTEGER IDENTITY(1,1) PRIMARY KEY,
            USER_ID INTEGER,
            MESSAGE_TEXT TEXT NOT NULL,
            IS_BOT BOOLEAN DEFAULT FALSE,
            TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            FOREIGN KEY (USER_ID) REFERENCES REGISTRATIONS(UID)
        )
        """)
        conn.commit()

def save_chat_message(conn, user_id, message, is_bot=False):
    """Save a chat message to the database"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO CHAT_HISTORY (USER_ID, MESSAGE_TEXT, IS_BOT)
            VALUES (%s, %s, %s)
            """, (user_id, message, is_bot))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error saving message: {str(e)}")
        return False

def get_chat_response(conn, message):
    """Generate a response to user message with RAG capabilities"""
    # Common responses for basic queries
    common_responses = {
        "hello": "Hello! How can I help you with race registration today?",
        "hi": "Hi there! Need help with race registration?",
        "event": "You can view all upcoming events in the Events tab. Would you like to know more about a specific event?",
        "register": "To register for an event, first make sure you're logged in, then go to the Events tab and click the Register button next to the event you're interested in.",
        "help": "I can help you with registration, finding events, and answering questions about the race system. What would you like to know?",
    }
    
    message_lower = message.lower()
    
    # Check for exact matches in common responses
    for key, response in common_responses.items():
        if key in message_lower:
            return response
    
    # Try to get an enhanced response using RAG
    enhanced_response = get_enhanced_response(conn, message)
    if enhanced_response:
        return enhanced_response
            
    # Fall back to default response
    return "I'm here to help with race registration and event information. Could you please be more specific about what you'd like to know?"

def chat_interface(conn):
    """Display the chat interface in the sidebar"""
    st.header("Ski Town Race Chat")
    
    # Initialize chat history in session state if not present
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []

    # Initialize submitted flag in session state
    if 'message_submitted' not in st.session_state:
        st.session_state.message_submitted = False
        
    # Chat input form
    with st.form(key='chat_form', clear_on_submit=True):
        user_input = st.text_input("Type your message:", key="chat_input")
        submit_button = st.form_submit_button("Send")
        
        if submit_button and user_input:
            st.session_state.message_submitted = True
            st.session_state.last_message = user_input
    
    # Handle submitted message
    if st.session_state.message_submitted:
        # Get user ID if logged in
        user_id = None
        if st.session_state.logged_in:
            user_id = get_user_id(conn, st.session_state.user_email)
        
        # Add user message to history
        st.session_state.chat_history.append({
            'text': st.session_state.last_message,
            'is_bot': False
        })
        
        # Save user message to database if logged in
        if user_id:
            save_chat_message(conn, user_id, st.session_state.last_message)
        
        # Get and display bot response
        bot_response = get_chat_response(conn, st.session_state.last_message)
        st.session_state.chat_history.append({
            'text': bot_response,
            'is_bot': True
        })
        
        # Save bot response to database if user is logged in
        if user_id:
            save_chat_message(conn, user_id, bot_response, is_bot=True)
        
        # Reset submitted flag
        st.session_state.message_submitted = False
        
        # Rerun to update display
        st.rerun()
    
    # Display chat history
    chat_container = st.container()
    with chat_container:
        for message in st.session_state.chat_history:
            if message['is_bot']:
                st.write("🤖 Assistant: " + message['text'])
            else:
                st.write("👤 You: " + message['text'])

with st.sidebar:
    st.logo("images/logo.png", size="large", icon_image=None)
    st.image("images/skitownrace.png")
    st.header("Rules")
    st.write("There is only one rule. There are no rules, other than your whole team has to cross the line.")
    st.write(" ")
    st.write(" ")
    url="https://www.usskiandsnowboard.org/safesport-athlete-safety"
    st.markdown("...and then there are [these](%s)" % url)
    st.write(" ") 
    st.write(" ")
    st.write(" ") 

    # CHAT GOES HERE     
    chat_interface(conn)

    st.write(" ") 
    st.caption("Made possible through support from ski town community leaders.")

    # 2 rows of images, 3 columns each
    # Row 1
    col1, col2, col3 = st.columns(3)
    with col1:
        st.image("images/resort/1.png", width=50)
    with col2:
        st.image("images/resort/2.png", width=50)
    with col3:
        st.image("images/resort/3.svg", width=50)
    
    # Row 2
    #col4, col5, col6 = st.columns(3)
    #with col4:
    #    st.image("images/resort/5.png", width=50)
    #with col5:
    #    st.image("images/resort/4.png", width=50)
    #with col6:
    #    st.image("images/resort/6.jpg", width=50)
    st.write(" ")
    st.write(" ") 
    st.write(" ") 
    st.write(" ") 
    st.write(" ") 
    st.caption("Copyright © 2025 SkiTownRace.com. All rights reserved.")


























# Main app logic
def main():
    #st.set_page_config(layout="wide")
    if not st.session_state.logged_in:
        st.title("Welcome to SkiTownRace.com")
        st.write("Register and then login to sign up for events. As you register for new events your profile information will be used to qualify you for events and suggest new ones.")
        st.write("Be sure your profile is up to date and accurate information in your profile.")
        # Tabs for login and registration
        tab1, tab2, tab3 = st.tabs(["Events", "Register", "Login"])
        
        with tab1:
            st.header("Events")
            
            # Filters
            col1, col2 = st.columns(2)
            with col1:
                search = st.text_input("Search events", placeholder="Enter event name, city, or venue...")
            with col2:
                discipline_filter = st.selectbox(
                    "Filter by Discipline",
                    options=["All"] + ["Alpine", "Combined Alpine", "Downhill", "Giant Slalom", "Slalom", "Super G"]
                )
            
            # Get filtered events
            discipline = None if discipline_filter == "All" else discipline_filter
            events = get_upcoming_events(conn, 
                                    search_term=search if search else None,
                                    discipline_filter=discipline)
            
            if events is not None:
                for _, row in events.iterrows():
                    st.write("---")
                    st.subheader(row['Event Name'])
                    st.write(f"**Discipline:** {row['Discipline']}")
                    st.write(f"**Venue:** {row['Venue']}")
                    st.write(f"**Competitors:** {row['Competitor Count']}")
                    st.caption(f"**Location:** {row['City']}, {row['State']}")
                    if pd.notna(row['URL']):
                        st.caption(f"**More Info:** [{row['URL']}]({row['URL']})")
            else:
                st.write("No upcoming events found.")

        with tab2:
            st.header("Register")
            with st.form("registration_form"):
                #member_id = st.text_input("Member ID", disabled=True)
                us_id = st.text_input("US Ski and Snowboard ID (optional)")
                fis_id = st.text_input("FIS ID (optional)")
                email = st.text_input("Email*")
                password = st.text_input("Password*", type="password")
                first_name = st.text_input("First Name*")
                last_name = st.text_input("Last Name*")
                dob = st.date_input("Date of Birth*")
                division = st.selectbox(
                    "Division*",
                    ("Alaska", "Central", "Eastern", "Far West", "Foreign", "Intermountain", "Northern", "Pacific Northwest", "Rocky"),
                    index=None,
                    placeholder="Select your division"
                )
                team = st.text_input("Team*")
                discipline = st.multiselect(
                    "Discipline*", 
                    options=["Alpine", "Combined Alpine", "Downhhill", "Giant Slalom", "Slalom", "Super G"]
                )
                
                submit = st.form_submit_button("Register")

                if submit:
                    # First, strip any whitespace from text inputs
                    email = email.strip() if email else ""
                    password = password.strip() if password else ""
                    first_name = first_name.strip() if first_name else ""
                    last_name = last_name.strip() if last_name else ""
                    team = team.strip() if team else ""
                    
                    # Check only the required fields (US_ID and FIS_ID are optional)
                    required_fields_present = all([
                        email,
                        password,
                        first_name,
                        last_name,
                        dob,
                        division,
                        discipline  # This is a multiselect, so it will be a list
                    ])
                    
                    if required_fields_present:
                        success = register_user(
                            conn=conn,
                            us_id=us_id,
                            fis_id=fis_id,
                            email=email,
                            password=password,
                            first_name=first_name,
                            last_name=last_name,
                            dob=dob,
                            division=division,
                            team=team,
                            discipline=discipline
                        )
                    else:
                        st.error("Please fill in all required fields marked with *")
        
        with tab3:
            st.header("Login")
            with st.form("login_form"):
                login_email = st.text_input("Email")
                login_password = st.text_input("Password", type="password")
                login_submitted = st.form_submit_button("Login")
                
                if login_submitted:
                    success, user_name = verify_login(conn, login_email, login_password)
                    if success:
                        st.session_state.logged_in = True
                        st.session_state.user_email = login_email
                        st.rerun()
                    else:
                        st.error("Invalid email or password")

    else:
        # Create tabs for different sections
        profile_tab, events_tab, new_event_tab = st.tabs(["Profile", "Events", "New Event"])
        
        with new_event_tab:
            with new_event_tab:
                st.header("New Event")

                # Get user information
                user_info = get_user_info(conn, st.session_state.user_email)
                user_id = get_user_id(conn, st.session_state.user_email)
                # Show Create Event button to all logged-in users
                st.markdown("---")
                # Update the event creation form handling in main():
                st.subheader("Create New Event")
            
            with st.form("create_event_form", clear_on_submit=True):
                event_name = st.text_input("Event Name*")
                event_date = st.date_input("Event Date*")
                location = st.text_input("Location (optional)")
                city = st.text_input("City*")
                state = st.text_input("State (2-letter code)*", max_chars=2)
                zip_code = st.text_input("ZIP Code*")
                venue = st.text_input("Venue*")
                discipline = st.selectbox(
                    "Discipline*",
                    options=["Alpine", "Combined Alpine", "Downhill", "Giant Slalom", "Slalom", "Super G"]
                )
                division = st.selectbox(
                    "Division*",
                    options=["Alaska", "Central", "Eastern", "Far West", "Foreign", "Intermountain", "Northern", "Pacific Northwest", "Rocky"],
                    index=None,
                    placeholder="Select division"
                )
                url = st.text_input("Registration URL (optional)")
                
                submit = st.form_submit_button("Create Event")
                
                if submit:
                    if all([event_name, event_date, city, state, zip_code, venue, discipline, division, user_id]):
                        st.write("Debug: All required fields present")
                        st.write(f"Debug: User ID: {user_id}")
                        
                        success = add_event(
                            conn=conn,
                            event_name=event_name,
                            event_date=event_date,
                            location=location,
                            city=city,
                            state=state,
                            zip_code=zip_code,
                            venue=venue,
                            discipline=discipline,
                            division=division,
                            creator_id=user_id,
                            url=url
                        )
                        
                        if success:
                            st.success("Event created successfully!")
                            st.rerun()
                        else:
                            st.error("Failed to create event. Please check the error messages above.")
                    else:
                        st.error("Please fill in all required fields (marked with *)")

            
        with profile_tab:
            #st.header("Profile")
            
            # Get user information
            user_info = get_user_info(conn, st.session_state.user_email)
            user_id = get_user_id(conn, st.session_state.user_email)
            
            if user_info:
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.header(f"Welcome, {user_info['Name']}!")
                    st.subheader("Your Registration Details")
                    for key, value in user_info.items():
                        if key != 'Name':  # Skip name since we already showed it
                            st.write(f"**{key}:** {value}")
                with col2:
                    if st.button("Logout", type="primary"):
                        logout()
            
         
            # Display user's registered events
            st.markdown("---")
            st.subheader("Your Registered Events")
    
            user_events = get_user_events(conn, user_id)
            if user_events is not None:
                # Configure the dataframe display with bib numbers
                st.dataframe(
                    user_events,
                    column_config={
                        "URL": st.column_config.LinkColumn("Event Link"),
                        "Competitor Count": st.column_config.NumberColumn(
                            "Competitors",
                            help="Number of registered competitors"
                        ),
                        "Bib Number": st.column_config.NumberColumn(
                            "Bib #",
                            help="Your assigned bib number for this event"
                        ),
                        "Event Date": st.column_config.DateColumn(
                            "Date",
                            format="MM/DD/YYYY"
                        )
                    },
                    hide_index=True,
                    use_container_width=True
                )
            else:
                st.write("You haven't registered for any events yet.")
            
            # Show events created by this user
            st.markdown("---")
            events_df = get_upcoming_events(conn)
            if events_df is not None:

                st.subheader("Events You Created")
                # Filter events created by the current user
                user_created_events = events_df[events_df['Creator ID'] == user_id]
                if not user_created_events.empty:
                    # Add edit buttons for each event
                    for index, event in user_created_events.iterrows():
                        with st.expander(f"🍺 {event['Event Name']} - {event['Event Date']}"):
                            st.write("**Event Details**")
                            with st.form(f"edit_event_form_{event['Event ID']}", clear_on_submit=True):
                                edited_name = st.text_input("Event Name*", value=event['Event Name'])
                                edited_date = st.date_input("Event Date*", value=event['Event Date'])
                                edited_location = st.text_input("Location", value=event['Location'] if pd.notna(event['Location']) else "")
                                edited_city = st.text_input("City*", value=event['City'])
                                edited_state = st.text_input("State (2-letter code)*", value=event['State'], max_chars=2)
                                edited_zip = st.text_input("ZIP Code*", value=event['ZIP'])
                                edited_venue = st.text_input("Venue*", value=event['Venue'])
                                edited_discipline = st.selectbox(
                                    "Discipline*",
                                    options=["Alpine", "Combined Alpine", "Downhill", "Giant Slalom", "Slalom", "Super G"],
                                    index=["Alpine", "Combined Alpine", "Downhill", "Giant Slalom", "Slalom", "Super G"].index(event['Discipline'])
                                )
                                edited_division = st.selectbox(
                                    "Division*",
                                    options=["Alaska", "Central", "Eastern", "Far West", "Foreign", "Intermountain", "Northern", "Pacific Northwest", "Rocky"],
                                    index=["Alaska", "Central", "Eastern", "Far West", "Foreign", "Intermountain", "Northern", "Pacific Northwest", "Rocky"].index(event['Division'])
                                )
                                edited_url = st.text_input("Registration URL", value=event['URL'] if pd.notna(event['URL']) else "")
                                
                                col1, col2 = st.columns([1, 4])
                                with col1:
                                    if st.form_submit_button("Save Changes"):
                                        if all([edited_name, edited_date, edited_city, edited_state, edited_zip, edited_venue, edited_discipline, edited_division]):
                                            if edit_event(
                                                conn=conn,
                                                event_id=event['Event ID'],
                                                event_name=edited_name,
                                                event_date=edited_date,
                                                location=edited_location,
                                                city=edited_city,
                                                state=edited_state,
                                                zip_code=edited_zip,
                                                venue=edited_venue,
                                                discipline=edited_discipline,
                                                division=edited_division,
                                                url=edited_url
                                            ):
                                                st.success("Event updated successfully!")
                                                st.rerun()
                                            else:
                                                st.error("Failed to update event.")
                                        else:
                                            st.error("Please fill in all required fields.")
                                with col2:
                                    if st.form_submit_button("🗑️ Delete Event"):
                                        if delete_event(conn, event['Event ID']):
                                            st.success("Event deleted successfully!")
                                            st.rerun()
                                        else:
                                            st.error("Failed to delete event.")
                else:
                    st.write("You haven't created any events yet.")
            

        with events_tab:
            st.header("Upcoming Events")
            
            # Filters
            col1, col2 = st.columns(2)
            with col1:
                search = st.text_input("Search events", placeholder="Enter event name, city, or venue...")
            with col2:
                discipline_filter = st.selectbox(
                    "Filter by Discipline",
                    options=["All"] + ["Alpine", "Combined Alpine", "Downhill", "Giant Slalom", "Slalom", "Super G"]
                )
            
            # Get filtered events
            discipline = None if discipline_filter == "All" else discipline_filter
            events = get_upcoming_events(conn, 
                                    search_term=search if search else None,
                                    discipline_filter=discipline)
            
            if events is not None:
                for _, row in events.iterrows():
                    st.write("---")
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.subheader(row['Event Name'])
                        st.write(f"**Venue:** {row['Venue']}")
                        st.write(f"**Location:** {row['City']}, {row['State']}")
                        st.write(f"**Discipline:** {row['Discipline']}")
                        st.write(f"**Competitors:** {row['Competitor Count']}")
                        if pd.notna(row['URL']):
                            st.write(f"**More Info:** [{row['URL']}]({row['URL']})")
                    
                    with col2:
                        event_id = row['Event ID']
                        is_registered = user_events is not None and event_id in user_events['Event ID'].values
                        
                        if is_registered:
                            if st.button("Unregister", key=f"unreg_{event_id}"):
                                if unregister_from_event(conn, event_id, user_id):
                                    st.success("Successfully unregistered!")
                                    st.rerun()
                        else:
                            if st.button("Register", key=f"reg_{event_id}"):
                                if register_for_event(conn, event_id, user_id):
                                    st.success("Successfully registered!")
                                    st.rerun()
            else:
                st.write("No upcoming events found.")

if __name__ == "__main__":
    main()
