import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime
import PyPDF2
import os

# Page configuration
st.set_page_config(
    page_title="US Ski & Snowboard Ski Town Race - www.skitownrace.com",
    page_icon="❄️",
    layout="wide",
)

# Custom CSS
st.markdown(
    """
<style>
    [data-testid=stSidebar] {
        background-color: #0F1D41;
        color: #ffffff
    }
    [data-testid=stSelectbox] [data-testid=stWidgetLabel] {
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
""",
    unsafe_allow_html=True,
)

# Initialize session state variables
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "user_email" not in st.session_state:
    st.session_state.user_email = None
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "message_submitted" not in st.session_state:
    st.session_state.message_submitted = False

# Snowflake connection setup
@st.cache_resource
def create_connection():
    return snowflake.connector.connect(
        user=st.secrets["SNOWFLAKE_USERNAME"],
        password=st.secrets["SNOWFLAKE_PASSWORD"],
        account=st.secrets["SNOWFLAKE_ACCOUNT"],
        role=st.secrets["SNOWFLAKE_ROLE"],
        warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
        database=st.secrets["SNOWFLAKE_DATABASE"],
        schema=st.secrets["SNOWFLAKE_SCHEMA"],
    )

def setup_cortex_functions(conn):
    """Setup Cortex Search and Mistral chat function"""
    try:
        with conn.cursor() as cur:
            # Create the Mistral chat function
            cur.execute("""
            CREATE OR REPLACE FUNCTION MISTRAL_CHAT(
                prompt STRING,
                system_prompt STRING
            )
            RETURNS STRING
            LANGUAGE SQL
            AS
            $$
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    'mistral-large2',
                    IFF(system_prompt = '', prompt, CONCAT(system_prompt, ' ', prompt))
                )
            $$;
            """)
            
            # Test if the function works
            cur.execute("SELECT MISTRAL_CHAT('test', 'You are a helpful assistant.')")
            test_response = cur.fetchone()
            
            if test_response and test_response[0]:
                st.success("Mistral chat function created and tested successfully")
                return True
            else:
                st.error("Mistral chat function created but returned no response")
                return False
                
    except Exception as e:
        st.error(f"Error setting up Cortex functions: {str(e)}")
        return False
    
def create_vector_search_table(conn):
    """Create table for storing document embeddings for vector search"""
    try:
        with conn.cursor() as cur:
            # Create table for document embeddings
            cur.execute("""
            CREATE TABLE IF NOT EXISTS DOCUMENT_EMBEDDINGS (
                DOC_ID INTEGER IDENTITY(1,1) PRIMARY KEY,
                FILENAME VARCHAR NOT NULL,
                CONTENT TEXT,
                CONTENT_CHUNK TEXT,
                EMBEDDING VECTOR,
                TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """)
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error creating vector search table: {str(e)}")
        return False
    
def process_pdf_with_embeddings(conn, pdf_path, chunk_size=1000):
    """Process PDF and create embeddings for vector search"""
    try:
        with open(pdf_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            full_text = ""
            
            # Extract text from all pages
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"
            
            if not full_text.strip():
                st.error(f"No text extracted from {pdf_path}")
                return False
                
            # Split content into chunks and create embeddings
            chunks = [full_text[i:i + chunk_size] for i in range(0, len(full_text), chunk_size)]
            filename = os.path.basename(pdf_path)
            
            with conn.cursor() as cur:
                # Remove existing entries
                cur.execute("DELETE FROM DOCUMENT_EMBEDDINGS WHERE FILENAME = %s", (filename,))
                
                # Insert chunks with embeddings
                for chunk in chunks:
                    cur.execute(
                        "INSERT INTO DOCUMENT_EMBEDDINGS (FILENAME, CONTENT, CONTENT_CHUNK, EMBEDDING) "
                        "SELECT %s, %s, %s, CORTEX_EMBED(%s)",
                        (filename, full_text, chunk, chunk)
                    )
                
            conn.commit()
            st.success(f"Processed and embedded PDF: {filename}")
            return True
            
    except Exception as e:
        st.error(f"Error processing PDF with embeddings: {str(e)}")
        return False

def initialize_cortex_system(conn):
    """Initialize the Cortex system with comprehensive error handling"""
    try:
        # Test if Mistral chat is available
        with conn.cursor() as cur:
            # Test the function with all required parameters
            cur.execute("""
            SELECT MISTRAL_CHAT(
                CAST('Hello' AS VARCHAR(16777216)),
                CAST('You are a helpful assistant.' AS VARCHAR(16777216)),
                0.7,
                100
            )
            """)
            
            result = cur.fetchone()
            if result and result[0]:
                st.toast("Chat system initialized successfully", icon='🤖')
                return True
            else:
                st.error("Chat system initialization failed - no response from Mistral", icon='❌')
                return False
                
    except Exception as e:
        st.error(f"Error initializing chat system: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return False

# User/Event Registration Tables
def create_registration_table(conn):
    """Create all necessary database tables"""
    try:
        with conn.cursor() as cur:
            # Create REGISTRATIONS table
            cur.execute(
                """
            CREATE TABLE IF NOT EXISTS REGISTRATIONS (
                UID INTEGER IDENTITY(1,1) PRIMARY KEY,
                MEMBER_ID INTEGER DEFAULT 0,
                US_ID VARCHAR,
                FIS_ID VARCHAR,
                EMAIL VARCHAR NOT NULL UNIQUE,
                PASSWORD VARCHAR NOT NULL,
                FIRST_NAME VARCHAR NOT NULL,
                LAST_NAME VARCHAR NOT NULL,
                FULL_NAME VARCHAR NOT NULL,
                DOB DATE NOT NULL,
                DIVISION VARCHAR NOT NULL,
                TEAM VARCHAR,
                DISCIPLINE VARCHAR
            )
            """
            )

            # Create UPCOMING_EVENTS table
            cur.execute(
                """
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
                CREATOR_ID INTEGER,
                FEE DECIMAL(10,2),
                URL VARCHAR,
                FOREIGN KEY (CREATOR_ID) REFERENCES REGISTRATIONS(UID)
            )
            """
            )

            # Create EVENT_REGISTRATIONS table
            cur.execute(
                """
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
            """
            )

            # Create CHAT_HISTORY table
            cur.execute(
                """
            CREATE TABLE IF NOT EXISTS CHAT_HISTORY (
                MESSAGE_ID INTEGER IDENTITY(1,1) PRIMARY KEY,
                USER_ID INTEGER,
                MESSAGE_TEXT TEXT NOT NULL,
                IS_BOT BOOLEAN DEFAULT FALSE,
                TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                FOREIGN KEY (USER_ID) REFERENCES REGISTRATIONS(UID)
            )
            """
            )

            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error creating tables: {str(e)}")
        return False

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

# Events
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

#PDF
def create_pdf_tables(conn):
    """Create tables for storing PDF content"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
            DROP TABLE IF EXISTS PDF_DOCUMENTS
            """
            )

            cur.execute(
                """
            CREATE TABLE IF NOT EXISTS PDF_DOCUMENTS (
                DOC_ID INTEGER IDENTITY(1,1),
                FILENAME VARCHAR NOT NULL,
                CONTENT TEXT,
                SECTIONS TEXT,
                TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            )
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error creating PDF tables: {str(e)}")
        return False

def process_pdf(conn, pdf_path, section_size=1000):
    """Process a PDF and store its content with sections"""
    try:
        with open(pdf_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            full_text = ""

            # Extract text from all pages
            for page in reader.pages:
                text = page.extract_text()
                if text:  # Only add if text was successfully extracted
                    full_text += text + "\n"

            # Store in database - first check if file already exists
            filename = os.path.basename(pdf_path)
            with conn.cursor() as cur:
                # Check for existing file
                cur.execute(
                    "SELECT DOC_ID FROM PDF_DOCUMENTS WHERE FILENAME = %s", (filename,)
                )
                existing = cur.fetchone()

                if existing:
                    # Update existing record
                    cur.execute(
                        """
                    UPDATE PDF_DOCUMENTS
                    SET CONTENT = %s,
                        SECTIONS = %s,
                        TIMESTAMP = CURRENT_TIMESTAMP()
                    WHERE FILENAME = %s
                    """,
                        (full_text, full_text, filename),
                    )
                else:
                    # Insert new record
                    cur.execute(
                        """
                    INSERT INTO PDF_DOCUMENTS (FILENAME, CONTENT, SECTIONS)
                    VALUES (%s, %s, %s)
                    """,
                        (filename, full_text, full_text),
                    )

            conn.commit()
            st.success(f"Successfully processed PDF: {filename}")
            return True
    except Exception as e:
        st.error(f"Error processing PDF: {str(e)}")
        return False

def get_relevant_content(conn, query):
    """Get specific content from PDF based on query"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
            SELECT DISTINCT LEFT(CONTENT, 1000) as content_preview
            FROM PDF_DOCUMENTS
            WHERE CONTAINS(LOWER(CONTENT), LOWER(%s))
            LIMIT 3
            """,
                (query,),
            )

            results = cur.fetchall()
            return [row[0] for row in results] if results else []
    except Exception as e:
        st.error(f"Error retrieving content: {str(e)}")
        return []

def get_enhanced_response(conn, query):
    """Get response using retrieved content"""
    try:
        relevant_sections = get_relevant_content(conn, query)
        if not relevant_sections:
            return None

        # Create a response using the found content
        response = "Based on the available information:\n\n"
        for i, section in enumerate(relevant_sections, 1):
            # Truncate long sections and add a marker
            cleaned_section = section.strip()
            if len(cleaned_section) > 300:
                cleaned_section = cleaned_section[:300] + "..."
            response += f"{i}. {cleaned_section}\n\n"

        return response

    except Exception as e:
        st.error(f"Error generating response: {str(e)}")
        return None

def initialize_search(conn):
    """Initialize all application components"""
    try:
        # Create core tables
        create_registration_table(conn)

        # Create PDF tables and process documents
        create_pdf_tables(conn)

        # Process PDFs in data directory
        pdf_dir = "data"
        if os.path.exists(pdf_dir):
            pdfs_processed = False
            for filename in os.listdir(pdf_dir):
                if filename.lower().endswith(".pdf"):
                    if process_pdf(conn, os.path.join(pdf_dir, filename)):
                        pdfs_processed = True

            if pdfs_processed:
                st.success("PDF processing completed successfully")
        else:
            st.warning("No 'data' directory found for PDFs")

        return True
    except Exception as e:
        st.error(f"Error initializing app: {str(e)}")
        return False

def save_chat_message(conn, user_id, message, is_bot=False):
    """Save a chat message to the database"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
            INSERT INTO CHAT_HISTORY (USER_ID, MESSAGE_TEXT, IS_BOT)
            VALUES (%s, %s, %s)
            """,
                (user_id, message, is_bot),
            )
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error saving message: {str(e)}")
        return False

def get_chat_response(conn, message, system_prompt="You are a helpful ski racing assistant.", temperature=0.7, max_tokens=1000):
    """Generate response using Mistral"""
    try:
        with conn.cursor() as cur:
            # Use the SQL version that we know works
            cur.execute("""
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    'mistral-large2',
                    CONCAT(%s, ' ', %s)
                )
            """, (system_prompt, message))
            response = cur.fetchone()[0]
            return response
    except Exception as e:
        print(f"Error in chat response: {str(e)}")  # For debugging
        return "So sorry for the inconvenience, but I'm having trouble accessing the chat system and working in limited capacityat the moment. Could you please try again in a little while?"

def test_search(conn, query):
    """Test function to verify search functionality"""
    with conn.cursor() as cur:
        cur.execute(
            """
        SELECT LEFT(content, 1000) 
        FROM pdf_documents 
        WHERE content ILIKE %s
        """,
            (f"%{query}%",),
        )

        result = cur.fetchone()
        if result:
            print(f"Found content for '{query}': {result[0][:200]}...")
        else:
            print(f"No content found for '{query}'")

def debug_pdf_content(conn):
    """Debug function to check PDF content"""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT CONTENT FROM PDF_DOCUMENTS LIMIT 1")
            result = cur.fetchone()
            if result:
                print(f"Found content length: {len(result[0])}")
                print(f"Sample content: {result[0][:200]}")
                return True
            else:
                print("No content found in database")
                return False
    except Exception as e:
        print(f"Debug error: {str(e)}")
        return False

def chat_interface(conn):
    """Display the chat interface with Mistral Large 2 integration"""

    with st.form(key="chat_form", clear_on_submit=True):
        user_input = st.text_area("Ask Nickane about ski racing", key="chat_input")
        submit_button = st.form_submit_button("Submit")
        
        if submit_button and user_input:
            st.session_state.message_submitted = True
            st.session_state.last_message = user_input
    
    if st.session_state.message_submitted:
        user_id = None
        if st.session_state.logged_in:
            user_id = get_user_id(conn, st.session_state.user_email)
        
        # Add user message to history
        st.session_state.chat_history.append({
            "text": st.session_state.last_message,
            "is_bot": False
        })
        
        # Save user message if logged in
        if user_id:
            save_chat_message(conn, user_id, st.session_state.last_message)
        
        # Get and display bot response using Mistral Large 2
        bot_response = get_chat_response(conn, st.session_state.last_message)
        st.session_state.chat_history.append({
            "text": bot_response,
            "is_bot": True
        })
        
        # Save bot response if user is logged in
        if user_id:
            save_chat_message(conn, user_id, bot_response, is_bot=True)
        
        st.session_state.message_submitted = False
        st.rerun()


if "active_tab" not in st.session_state:
    st.session_state.active_tab = 0  # Default to first tab


def display_chat_history():
    """Display chat history in collapsible window"""
    # When new message is added, set active tab to Chat (index 3)
    if st.session_state.message_submitted:
        st.session_state.active_tab = 3
        
    for message in st.session_state.chat_history:
        if message["is_bot"]:
            st.markdown(f"""
                <div style='background-color: #f0f2f6; padding: 10px; border-radius: 5px; margin: 5px 0;'>
                    🤖 <b>Nickane:</b> {message["text"]}
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div style='background-color: #e8f0fe; padding: 10px; border-radius: 5px; margin: 5px 0;'>
                    👤 <b>You:</b> {message["text"]}
                </div>
                """, unsafe_allow_html=True)

# Test PDF content directly
def test_pdf_search(conn, query):
    """Test function to directly search PDF content"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
            SELECT CONTENT
            FROM PDF_DOCUMENTS
            WHERE CONTAINS(LOWER(CONTENT), LOWER(%s))
            LIMIT 1
            """,
                (query,),
            )
            result = cur.fetchone()
            if result:
                return f"Found matching content: {result[0][:200]}..."
            return "No matching content found"
    except Exception as e:
        return f"Error testing PDF search: {str(e)}"

def verify_pdf_content(conn):
    """Verify PDF content in database"""
    with conn.cursor() as cur:
        # Check if table exists
        cur.execute(
            """
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'PDF_DOCUMENTS'
        """
        )
        if cur.fetchone()[0] == 0:
            st.error("PDF_DOCUMENTS table does not exist!")
            return False

        # Check for content
        cur.execute("SELECT DOC_ID, FILENAME, LEFT(CONTENT, 100) FROM PDF_DOCUMENTS")
        rows = cur.fetchall()

        if not rows:
            st.error("No PDF content found in database!")
            return False

        for row in rows:
            st.write(f"Found document: {row[1]}")
            st.write(f"Preview: {row[2]}...")

        return True

def direct_pdf_search(conn, query):
    """Direct search in PDF content"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
            SELECT FILENAME, LEFT(CONTENT, 200)
            FROM PDF_DOCUMENTS
            WHERE CONTAINS(CONTENT, %s)
               OR CONTENT ILIKE %s
            """,
                (query, f"%{query}%"),
            )

            results = cur.fetchall()
            if results:
                for filename, preview in results:
                    st.write(f"Match in {filename}:")
                    st.write(preview + "...")
                return True
            else:
                st.write("No matches found.")
                return False
    except Exception as e:
        st.error(f"Search error: {str(e)}")
        return False

def test_chat_search(conn, query):
    """Test search functionality directly"""
    try:
        with conn.cursor() as cur:
            # Test simple ILIKE search
            cur.execute(
                """
            SELECT LEFT(content, 500)
            FROM pdf_documents
            WHERE content ILIKE %s
            LIMIT 1
            """,
                (f"%{query}%",),
            )

            result = cur.fetchone()
            if result:
                st.write(f"\nFound matching content for '{query}':")
                st.write(result[0])
            else:
                st.write(f"\nNo direct matches found for '{query}'")

        return True
    except Exception as e:
        st.error(f"Search test error: {str(e)}")
        return False

def diagnose_pdf_system(conn):
    """Diagnose PDF storage and search system"""
    try:
        st.write("Running PDF system diagnostics...")

        # 1. Check table structure
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'PDF_DOCUMENTS'
            """)
            columns = cur.fetchall()
            st.write("Table structure:")
            for col in columns:
                st.write(f"- {col[0]}: {col[1]}")

        # 2. Check document details with corrected query
        with conn.cursor() as cur:
            cur.execute("""
                SELECT filename, 
                       LENGTH(content) as content_length,
                       LEFT(content, 200) as content_preview
                FROM pdf_documents
            """)
            docs = cur.fetchall()

            st.write("\nDocument details:")
            for doc in docs:
                st.write(f"\nFilename: {doc[0]}")
                st.write(f"Content length: {doc[1]} characters")
                st.write(f"Preview: {doc[2]}...")

        return True
    except Exception as e:
        st.error(f"Diagnostic error: {str(e)}")
        return False

# Initialization code to verify the PDFs
def initialize_search_system(conn):
    """Initialize and verify search system"""
    try:
        # First create tables
        create_pdf_tables(conn)

        # Process PDFs
        pdf_dir = "data"
        if not os.path.exists(pdf_dir):
            st.error(f"Directory not found: {pdf_dir}")
            return False

        # Process each PDF
        for filename in os.listdir(pdf_dir):
            if filename.lower().endswith(".pdf"):
                file_path = os.path.join(pdf_dir, filename)
                process_pdf(conn, file_path)

        # Verify content
        if not verify_pdf_content(conn):
            st.error("PDF content verification failed!")
            return False

        # Test search
        st.write("Testing search functionality...")
        direct_pdf_search(conn, "alpine")

        return True
    except Exception as e:
        st.error(f"Error initializing search: {str(e)}")
        return False

def process_pdf(conn, pdf_path):
    """Process a PDF and store its content"""
    try:
        with open(pdf_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            full_text = ""

            # Extract text from all pages
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"

            if not full_text.strip():
                st.error(f"No text extracted from {pdf_path}")
                return False

            # Store in database
            with conn.cursor() as cur:
                # Remove existing entry if any
                cur.execute(
                    "DELETE FROM PDF_DOCUMENTS WHERE FILENAME = %s",
                    (os.path.basename(pdf_path),),
                )

                # Insert new content
                cur.execute(
                    """
                INSERT INTO PDF_DOCUMENTS (FILENAME, CONTENT)
                VALUES (%s, %s)
                """,
                    (os.path.basename(pdf_path), full_text),
                )

            conn.commit()
            st.success(f"Processed PDF: {os.path.basename(pdf_path)}")
            st.write(f"Extracted {len(full_text)} characters")
            return True

    except Exception as e:
        st.error(f"Error processing PDF: {str(e)}")
        return False

# Event registration
def get_upcoming_events(
    conn, search_term=None, state_filter=None, discipline_filter=None
):
    """Get upcoming events with optional filters"""
    try:
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

            cur.execute(query, tuple(params) if params else ())
            rows = cur.fetchall()
            if rows:
                return pd.DataFrame(
                    rows,
                    columns=[
                        "Event ID",
                        "Event Name",
                        "Event Date",
                        "Competitor Count",
                        "Location",
                        "City",
                        "State",
                        "ZIP",
                        "Venue",
                        "Division",
                        "Discipline",
                        "URL",
                        "Creator ID",
                    ],
                )
            return None
    except Exception as e:
        st.error(f"Error retrieving events: {str(e)}")
        return None

def register_for_event(conn, event_id, user_id):
    """Register a user for an event"""
    try:
        with conn.cursor() as cur:
            # Get the next available bib number for this event
            cur.execute(
                """
            SELECT COALESCE(MAX(BIB_NUMBER), 0) + 1
            FROM EVENT_REGISTRATIONS
            WHERE EVENT_ID = %s
            """,
                (event_id,),
            )
            next_bib = cur.fetchone()[0]

            # Insert the new registration with bib number
            cur.execute(
                """
            INSERT INTO EVENT_REGISTRATIONS (EVENT_ID, USER_ID, BIB_NUMBER)
            VALUES (%s, %s, %s)
            """,
                (event_id, user_id, next_bib),
            )

            # Update competitor count
            cur.execute(
                """
            UPDATE UPCOMING_EVENTS 
            SET COMPETITOR_COUNT = (
                SELECT COUNT(*) 
                FROM EVENT_REGISTRATIONS 
                WHERE EVENT_ID = %s
            )
            WHERE EVENT_ID = %s
            """,
                (event_id, event_id),
            )

            conn.commit()
            return True
    except Exception as e:
        if "duplicate key value violates unique constraint" in str(e).lower():
            st.error("You are already registered for this event.")
        else:
            st.error(f"Error registering for event: {e}")
        return False

def unregister_from_event(conn, event_id, user_id):
    """Unregister a user from an event"""
    try:
        with conn.cursor() as cur:
            # Remove registration
            cur.execute(
                """
            DELETE FROM EVENT_REGISTRATIONS 
            WHERE EVENT_ID = %s AND USER_ID = %s
            """,
                (event_id, user_id),
            )

            # Update competitor count
            cur.execute(
                """
            UPDATE UPCOMING_EVENTS
            SET COMPETITOR_COUNT = (
                SELECT COUNT(*) 
                FROM EVENT_REGISTRATIONS 
                WHERE EVENT_ID = %s
            )
            WHERE EVENT_ID = %s
            """,
                (event_id, event_id),
            )

            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error unregistering from event: {e}")
        return False

def get_user_events(conn, user_id):
    """Get all events a user is registered for"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
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
            """,
                (user_id,),
            )

            rows = cur.fetchall()
            if rows:
                return pd.DataFrame(
                    rows,
                    columns=[
                        "Event ID",
                        "Event Name",
                        "Event Date",
                        "Competitor Count",
                        "Location",
                        "City",
                        "State",
                        "ZIP",
                        "Venue",
                        "Discipline",
                        "URL",
                        "Registration Date",
                        "Bib Number",
                    ],
                )
            return None
    except Exception as e:
        st.error(f"Error fetching user events: {e}")
        return None

# Connect to Snowflake and initialize
conn = create_connection()
create_registration_table(conn)

#if initialize_search_system(conn):
# st.success("Search system initialized successfully")
# Verify content is searchable
# debug_pdf_content(conn)
#diagnose_pdf_system(conn)
# test_chat_search(conn, "age groups")



# Initialize Cortex system
if initialize_cortex_system(conn):
    st.toast("Cortex chat system is ready", icon='🧠')
else:
    with st.sidebar:
        st.error("Failed to initialize chat system", icon='❌')

        if st.button("Run PDF System Diagnostics"):
            diagnose_pdf_system(conn)



# Main app logic with existing registration and event management code...
def main():
    # Sidebar configuration
    with st.sidebar:
        st.image("images/skitownrace.png")
        st.header("Rules")
        st.write(
            "There is only one rule. There are no rules, other than your whole team has to cross the line."
        )
        st.write(" ")
        st.write(" ")
        url = "https://www.usskiandsnowboard.org/safesport-athlete-safety"
        st.markdown("...and then there are [these](%s)" % url)
        st.write(" ")
        st.write(" ")
        st.write(" ")

        st.write(" ")
        st.caption("Made possible through support from ski town community leaders.")

        # Sponsor logos
        col1, col2, col3 = st.columns(3)
        with col1:
            st.image("images/resort/1.png", width=50)
        with col2:
            st.image("images/resort/2.png", width=50)
        with col3:
            st.image("images/resort/3.svg", width=50)
        
        # st.button("Run PDF System Diagnostics", type="primary") 
        st.write(" ")
        st.caption("Copyright © 2025 SkiTownRace.com. All rights reserved.")

    if not st.session_state.logged_in:
        st.title("Welcome to SkiTownRace.com")
        st.write(
            "Register and then login to sign up for events. As you register for new events your profile information will be used to qualify you for events and suggest new ones."
        )
        st.write(
            "Be sure your profile is up to date and accurate information in your profile."
        )
        


        # Tabs for login and registration
        tab1, tab2, tab3, tab4 = st.tabs(["Events", "Register", "Login", "Chat"])

        with tab1:
            display_events_tab(conn)
        with tab2:
            display_registration_tab(conn)
        with tab3:
            display_login_tab(conn)
        with tab4:
            chat_interface(conn)
            display_chat_history()

    else:
        

        # Create tabs for different sections
        tab1, tab2, tab3, tab4 = st.tabs(["Profile", "Events", "New Event", "Chat"])

        with tab1:
            display_profile_tab(conn)
    
        with tab2:
            display_events_tab(conn, show_registration=True)
    
        with tab3:
            display_new_event_tab(conn)

        with tab4:
            chat_interface(conn)
            display_chat_history()

def display_events_tab(conn, show_registration=False):
    st.header("Upcoming Events")

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        search = st.text_input(
            "Search events", placeholder="Enter event name, city, or venue..."
        )
    with col2:
        discipline_filter = st.selectbox(
            "Filter by Discipline",
            options=["All"]
            + [
                "Alpine",
                "Combined Alpine",
                "Downhill",
                "Giant Slalom",
                "Slalom",
                "Super G",
            ],
        )

    # Get filtered events
    discipline = None if discipline_filter == "All" else discipline_filter
    events = get_upcoming_events(
        conn, search_term=search if search else None, discipline_filter=discipline
    )

    if events is not None:
        for _, row in events.iterrows():
            st.write("---")
            col1, col2 = st.columns([3, 1])

            with col1:
                st.subheader(row["Event Name"])
                st.write(f"**Date:** {row['Event Date'].strftime('%B %d, %Y')}")
                st.write(f"**Venue:** {row['Venue']}")
                st.write(f"**Location:** {row['City']}, {row['State']}")
                st.write(f"**Discipline:** {row['Discipline']}")
                st.write(f"**Competitors:** {row['Competitor Count']}")
                if pd.notna(row["URL"]):
                    st.write(f"**More Info:** [{row['URL']}]({row['URL']})")

            if show_registration and st.session_state.logged_in:
                with col2:
                    event_id = row["Event ID"]
                    user_id = get_user_id(conn, st.session_state.user_email)
                    user_events = get_user_events(conn, user_id)
                    is_registered = (
                        user_events is not None
                        and event_id in user_events["Event ID"].values
                    )

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


def display_registration_tab(conn):
    st.header("Register")
    with st.form("registration_form"):
        us_id = st.text_input("US Ski and Snowboard ID (optional)")
        fis_id = st.text_input("FIS ID (optional)")
        email = st.text_input("Email*")
        password = st.text_input("Password*", type="password")
        first_name = st.text_input("First Name*")
        last_name = st.text_input("Last Name*")
        dob = st.date_input("Date of Birth*")
        division = st.selectbox(
            "Division*",
            (
                "Alaska",
                "Central",
                "Eastern",
                "Far West",
                "Foreign",
                "Intermountain",
                "Northern",
                "Pacific Northwest",
                "Rocky",
            ),
            index=None,
            placeholder="Select your division",
        )
        team = st.text_input("Team (optional)")
        discipline = st.multiselect(
            "Discipline*",
            options=[
                "Alpine",
                "Combined Alpine",
                "Downhill",
                "Giant Slalom",
                "Slalom",
                "Super G",
            ],
        )

        submit = st.form_submit_button("Register")

        if submit:
            # Strip whitespace from text inputs
            email = email.strip() if email else ""
            password = password.strip() if password else ""
            first_name = first_name.strip() if first_name else ""
            last_name = last_name.strip() if last_name else ""
            team = team.strip() if team else ""

            required_fields_present = all(
                [email, password, first_name, last_name, dob, division, discipline]
            )

            if required_fields_present:
                register_user(
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
                    discipline=discipline,
                )
            else:
                st.error("Please fill in all required fields marked with *")


def display_login_tab(conn):
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


def display_profile_tab(conn):
    user_info = get_user_info(conn, st.session_state.user_email)
    user_id = get_user_id(conn, st.session_state.user_email)

    if user_info:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.header(f"Welcome, {user_info['Name']}!")
            st.subheader("Your Registration Details")
            for key, value in user_info.items():
                if key != "Name":  # Skip name since we already showed it
                    st.write(f"**{key}:** {value}")
        with col2:
            if st.button("Logout", type="primary"):
                logout()

    # Display user's registered events
    st.markdown("---")
    st.subheader("Your Registered Events")
    user_events = get_user_events(conn, user_id)

    if user_events is not None:
        st.dataframe(
            user_events,
            column_config={
                "URL": st.column_config.LinkColumn("Event Link"),
                "Competitor Count": st.column_config.NumberColumn(
                    "Competitors", help="Number of registered competitors"
                ),
                "Bib Number": st.column_config.NumberColumn(
                    "Bib #", help="Your assigned bib number for this event"
                ),
                "Event Date": st.column_config.DateColumn("Date", format="MM/DD/YYYY"),
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.write("You haven't registered for any events yet.")


def display_new_event_tab(conn):
    st.header("Create New Event")
    user_id = get_user_id(conn, st.session_state.user_email)

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
            options=[
                "Alpine",
                "Combined Alpine",
                "Downhill",
                "Giant Slalom",
                "Slalom",
                "Super G",
            ],
        )
        division = st.selectbox(
            "Division*",
            options=[
                "Alaska",
                "Central",
                "Eastern",
                "Far West",
                "Foreign",
                "Intermountain",
                "Northern",
                "Pacific Northwest",
                "Rocky",
            ],
            index=None,
            placeholder="Select division",
        )
        url = st.text_input("Registration URL (optional)")

        submit = st.form_submit_button("Create Event")

        if submit:
            if all(
                [
                    event_name,
                    event_date,
                    city,
                    state,
                    zip_code,
                    venue,
                    discipline,
                    division,
                ]
            ):
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
                    url=url,
                )

                if success:
                    st.success("Event created successfully!")
                    st.rerun()
            else:
                st.error("Please fill in all required fields (marked with *)")


if __name__ == "__main__":
    main()
