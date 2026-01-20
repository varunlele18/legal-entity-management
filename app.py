import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, date
import uuid
from pathlib import Path

# Page configuration
st.set_page_config(
    page_title="Legal Entity Management",
    page_icon="🏢",
    layout="wide"
)

# Database setup
DB_FILE = "legal_entities.db"

def init_database():
    """Initialize SQLite database with tables"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Legal Entities table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS legal_entities (
            abn TEXT PRIMARY KEY,
            entity_name TEXT NOT NULL,
            parent_abn TEXT,
            entity_type TEXT,
            status TEXT DEFAULT 'Active',
            effective_date DATE NOT NULL,
            end_date DATE,
            created_by TEXT,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            modified_by TEXT,
            modified_date TIMESTAMP
        )
    """)
    
    # Reporting Types table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reporting_types (
            reporting_type_id TEXT PRIMARY KEY,
            reporting_type_name TEXT NOT NULL,
            description TEXT,
            is_active BOOLEAN DEFAULT 1,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Sector Codes table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sector_codes (
            sector_code TEXT PRIMARY KEY,
            sector_name TEXT NOT NULL,
            sector_description TEXT,
            is_active BOOLEAN DEFAULT 1,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Sector ABN Mapping table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sector_abn_mapping (
            mapping_id TEXT PRIMARY KEY,
            reporting_type_id TEXT NOT NULL,
            sector_code TEXT NOT NULL,
            abn TEXT NOT NULL,
            effective_date DATE NOT NULL,
            end_date DATE,
            is_active BOOLEAN DEFAULT 1,
            created_by TEXT,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            modified_by TEXT,
            modified_date TIMESTAMP,
            FOREIGN KEY (reporting_type_id) REFERENCES reporting_types(reporting_type_id),
            FOREIGN KEY (sector_code) REFERENCES sector_codes(sector_code),
            FOREIGN KEY (abn) REFERENCES legal_entities(abn)
        )
    """)
    
    conn.commit()
    conn.close()

# Helper functions
def load_data(table_name):
    """Load data from SQLite table"""
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading {table_name}: {str(e)}")
        return pd.DataFrame()

def execute_query(query, params=None):
    """Execute SQL query"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return False

def delete_record(table_name, key_column, key_value):
    """Delete a record from table"""
    query = f"DELETE FROM {table_name} WHERE {key_column} = ?"
    return execute_query(query, (key_value,))

# Initialize database
init_database()

# Insert sample data if tables are empty
def insert_sample_data():
    """Insert sample data for demo purposes"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Check if data exists
    cursor.execute("SELECT COUNT(*) FROM legal_entities")
    if cursor.fetchone()[0] == 0:
        # Insert sample reporting types
        cursor.executemany("""
            INSERT INTO reporting_types (reporting_type_id, reporting_type_name, description)
            VALUES (?, ?, ?)
        """, [
            ('FIN', 'Financial Reporting', 'Financial consolidation and reporting'),
            ('COM', 'Commercial Reporting', 'Commercial and operational reporting'),
            ('REG', 'Regulatory Reporting', 'Regulatory compliance reporting')
        ])
        
        # Insert sample sector codes
        cursor.executemany("""
            INSERT INTO sector_codes (sector_code, sector_name, sector_description)
            VALUES (?, ?, ?)
        """, [
            ('TECH', 'Technology', 'Technology and IT services'),
            ('FIN', 'Finance', 'Financial services'),
            ('MFG', 'Manufacturing', 'Manufacturing and production'),
            ('RET', 'Retail', 'Retail and consumer goods')
        ])
        
        # Insert sample entities
        cursor.executemany("""
            INSERT INTO legal_entities (abn, entity_name, parent_abn, entity_type, status, effective_date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            ('12345678901', 'Parent Corp Pty Ltd', None, 'Parent', 'Active', '2020-01-01'),
            ('23456789012', 'Tech Subsidiary Pty Ltd', '12345678901', 'Subsidiary', 'Active', '2021-06-01'),
            ('34567890123', 'Finance Division Pty Ltd', '12345678901', 'Subsidiary', 'Active', '2021-06-01')
        ])
        
        conn.commit()
    
    conn.close()

# Insert sample data
insert_sample_data()

# App UI
st.title("🏢 Legal Entity & Sector Code Management")
st.markdown("---")

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select Module",
    ["Legal Entity Hierarchy", "Sector Code Mappings", "Reference Data", "Reports & Analytics"]
)

# ============================================================================
# PAGE 1: LEGAL ENTITY HIERARCHY
# ============================================================================
if page == "Legal Entity Hierarchy":
    st.header("Legal Entity Hierarchy Management")
    
    tab1, tab2, tab3 = st.tabs(["View Hierarchy", "Add Entity", "Edit/Delete Entity"])
    
    # Tab 1: View Hierarchy
    with tab1:
        st.subheader("Current Legal Entity Hierarchy")
        
        entities_df = load_data("legal_entities")
        
        if not entities_df.empty:
            # Filter options
            col1, col2 = st.columns(2)
            with col1:
                status_filter = st.multiselect(
                    "Filter by Status",
                    options=entities_df['status'].unique(),
                    default=entities_df['status'].unique()
                )
            with col2:
                entity_type_filter = st.multiselect(
                    "Filter by Entity Type",
                    options=entities_df['entity_type'].unique(),
                    default=entities_df['entity_type'].unique()
                )
            
            filtered_df = entities_df[
                (entities_df['status'].isin(status_filter)) &
                (entities_df['entity_type'].isin(entity_type_filter))
            ]
            
            st.dataframe(
                filtered_df[['abn', 'entity_name', 'parent_abn', 'entity_type', 'status', 'effective_date']],
                use_container_width=True,
                hide_index=True
            )
            
            # Hierarchy visualization
            st.subheader("Hierarchy Tree View")
            parent_entities = filtered_df[filtered_df['parent_abn'].isna()]
            
            for _, parent in parent_entities.iterrows():
                with st.expander(f"📊 {parent['entity_name']} ({parent['abn']})"):
                    children = filtered_df[filtered_df['parent_abn'] == parent['abn']]
                    if not children.empty:
                        for _, child in children.iterrows():
                            st.write(f"  └─ {child['entity_name']} ({child['abn']}) - {child['entity_type']}")
        else:
            st.info("No legal entities found. Add your first entity in the 'Add Entity' tab.")
    
    # Tab 2: Add Entity
    with tab2:
        st.subheader("Add New Legal Entity")
        
        with st.form("add_entity_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                new_abn = st.text_input("ABN *", max_chars=11, help="11-digit ABN")
                new_entity_name = st.text_input("Entity Name *")
                new_entity_type = st.selectbox(
                    "Entity Type *",
                    ["Parent", "Subsidiary", "Branch", "Joint Venture", "Other"]
                )
            
            with col2:
                entities_df = load_data("legal_entities")
                parent_options = ["None"] + entities_df['abn'].tolist() if not entities_df.empty else ["None"]
                new_parent_abn = st.selectbox("Parent ABN", parent_options)
                new_status = st.selectbox("Status", ["Active", "Inactive", "Pending"])
                new_effective_date = st.date_input("Effective Date", value=date.today())
            
            submitted = st.form_submit_button("Add Legal Entity")
            
            if submitted:
                if not new_abn or len(new_abn) != 11:
                    st.error("Please enter a valid 11-digit ABN")
                elif not new_entity_name:
                    st.error("Please enter an entity name")
                else:
                    # Check if ABN already exists
                    if not entities_df.empty and new_abn in entities_df['abn'].values:
                        st.error(f"ABN {new_abn} already exists!")
                    else:
                        query = """
                            INSERT INTO legal_entities 
                            (abn, entity_name, parent_abn, entity_type, status, effective_date, created_by)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """
                        params = (
                            new_abn,
                            new_entity_name,
                            None if new_parent_abn == "None" else new_parent_abn,
                            new_entity_type,
                            new_status,
                            new_effective_date,
                            'streamlit_user'
                        )
                        
                        if execute_query(query, params):
                            st.success(f"Successfully added entity: {new_entity_name} ({new_abn})")
                            st.rerun()
    
    # Tab 3: Edit/Delete Entity
    with tab3:
        st.subheader("Edit or Delete Legal Entity")
        
        entities_df = load_data("legal_entities")
        
        if not entities_df.empty:
            selected_abn = st.selectbox(
                "Select Entity to Edit/Delete",
                entities_df['abn'].tolist(),
                format_func=lambda x: f"{entities_df[entities_df['abn']==x]['entity_name'].values[0]} ({x})"
            )
            
            if selected_abn:
                entity_data = entities_df[entities_df['abn'] == selected_abn].iloc[0]
                
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    with st.form("edit_entity_form"):
                        st.write("**Edit Entity Details**")
                        
                        edit_col1, edit_col2 = st.columns(2)
                        
                        with edit_col1:
                            edit_name = st.text_input("Entity Name", value=entity_data['entity_name'])
                            edit_type = st.selectbox(
                                "Entity Type",
                                ["Parent", "Subsidiary", "Branch", "Joint Venture", "Other"],
                                index=["Parent", "Subsidiary", "Branch", "Joint Venture", "Other"].index(entity_data['entity_type']) if entity_data['entity_type'] in ["Parent", "Subsidiary", "Branch", "Joint Venture", "Other"] else 0
                            )
                        
                        with edit_col2:
                            edit_status = st.selectbox("Status", ["Active", "Inactive", "Pending"], 
                                index=["Active", "Inactive", "Pending"].index(entity_data['status']) if entity_data['status'] in ["Active", "Inactive", "Pending"] else 0)
                        
                        update_submitted = st.form_submit_button("Update Entity")
                        
                        if update_submitted:
                            query = """
                                UPDATE legal_entities 
                                SET entity_name = ?, entity_type = ?, status = ?, modified_by = ?, modified_date = ?
                                WHERE abn = ?
                            """
                            params = (edit_name, edit_type, edit_status, 'streamlit_user', datetime.now(), selected_abn)
                            
                            if execute_query(query, params):
                                st.success("Entity updated successfully!")
                                st.rerun()
                
                with col2:
                    st.write("")
                    st.write("")
                    if st.button("🗑️ Delete Entity", type="secondary", use_container_width=True):
                        # Check if entity has children
                        children = entities_df[entities_df['parent_abn'] == selected_abn]
                        if not children.empty:
                            st.error("Cannot delete entity with child entities!")
                        else:
                            if delete_record("legal_entities", "abn", selected_abn):
                                st.success("Entity deleted successfully!")
                                st.rerun()

# ============================================================================
# PAGE 2: SECTOR CODE MAPPINGS
# ============================================================================
elif page == "Sector Code Mappings":
    st.header("Sector Code to ABN Mapping")
    
    tab1, tab2 = st.tabs(["View Mappings", "Add Mapping"])
    
    with tab1:
        st.subheader("Current Sector Code Mappings")
        
        # Load and join data
        conn = sqlite3.connect(DB_FILE)
        query = """
            SELECT 
                m.*,
                r.reporting_type_name,
                s.sector_name,
                e.entity_name
            FROM sector_abn_mapping m
            LEFT JOIN reporting_types r ON m.reporting_type_id = r.reporting_type_id
            LEFT JOIN sector_codes s ON m.sector_code = s.sector_code
            LEFT JOIN legal_entities e ON m.abn = e.abn
        """
        mappings_df = pd.read_sql_query(query, conn)
        conn.close()
        
        if not mappings_df.empty:
            st.dataframe(
                mappings_df[['reporting_type_name', 'sector_name', 'entity_name', 'abn', 'effective_date', 'is_active']],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No mappings found.")
    
    with tab2:
        st.subheader("Add New Sector Code Mapping")
        
        reporting_types_df = load_data("reporting_types")
        sector_codes_df = load_data("sector_codes")
        entities_df = load_data("legal_entities")
        
        if not reporting_types_df.empty and not sector_codes_df.empty and not entities_df.empty:
            with st.form("add_mapping_form"):
                col1, col2 = st.columns(2)
                
                with col1:
                    mapping_reporting_type = st.selectbox(
                        "Reporting Type *",
                        reporting_types_df['reporting_type_id'].tolist(),
                        format_func=lambda x: reporting_types_df[reporting_types_df['reporting_type_id']==x]['reporting_type_name'].values[0]
                    )
                    
                    mapping_sector = st.selectbox(
                        "Sector Code *",
                        sector_codes_df['sector_code'].tolist(),
                        format_func=lambda x: f"{sector_codes_df[sector_codes_df['sector_code']==x]['sector_name'].values[0]} ({x})"
                    )
                
                with col2:
                    mapping_abn = st.selectbox(
                        "ABN *",
                        entities_df['abn'].tolist(),
                        format_func=lambda x: f"{entities_df[entities_df['abn']==x]['entity_name'].values[0]} ({x})"
                    )
                    
                    mapping_effective_date = st.date_input("Effective Date", value=date.today())
                
                submitted = st.form_submit_button("Add Mapping")
                
                if submitted:
                    mapping_id = f"MAP{str(uuid.uuid4())[:8].upper()}"
                    
                    query = """
                        INSERT INTO sector_abn_mapping 
                        (mapping_id, reporting_type_id, sector_code, abn, effective_date, created_by)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """
                    params = (mapping_id, mapping_reporting_type, mapping_sector, mapping_abn, mapping_effective_date, 'streamlit_user')
                    
                    if execute_query(query, params):
                        st.success("Mapping added successfully!")
                        st.rerun()

# ============================================================================
# PAGE 3: REFERENCE DATA
# ============================================================================
elif page == "Reference Data":
    st.header("Reference Data Management")
    
    tab1, tab2 = st.tabs(["Reporting Types", "Sector Codes"])
    
    with tab1:
        st.subheader("Reporting Types")
        
        reporting_types_df = load_data("reporting_types")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            if not reporting_types_df.empty:
                st.dataframe(
                    reporting_types_df[['reporting_type_id', 'reporting_type_name', 'description', 'is_active']],
                    use_container_width=True,
                    hide_index=True
                )
        
        with col2:
            with st.form("add_reporting_type"):
                st.write("**Add New**")
                new_rt_id = st.text_input("Type ID", max_chars=10)
                new_rt_name = st.text_input("Type Name")
                new_rt_desc = st.text_area("Description")
                
                if st.form_submit_button("Add"):
                    if new_rt_id and new_rt_name:
                        query = "INSERT INTO reporting_types (reporting_type_id, reporting_type_name, description) VALUES (?, ?, ?)"
                        if execute_query(query, (new_rt_id, new_rt_name, new_rt_desc)):
                            st.success("Added!")
                            st.rerun()
    
    with tab2:
        st.subheader("Sector Codes")
        
        sector_codes_df = load_data("sector_codes")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            if not sector_codes_df.empty:
                st.dataframe(
                    sector_codes_df[['sector_code', 'sector_name', 'sector_description', 'is_active']],
                    use_container_width=True,
                    hide_index=True
                )
        
        with col2:
            with st.form("add_sector_code"):
                st.write("**Add New**")
                new_sc_code = st.text_input("Code", max_chars=10)
                new_sc_name = st.text_input("Name")
                new_sc_desc = st.text_area("Description")
                
                if st.form_submit_button("Add"):
                    if new_sc_code and new_sc_name:
                        query = "INSERT INTO sector_codes (sector_code, sector_name, sector_description) VALUES (?, ?, ?)"
                        if execute_query(query, (new_sc_code, new_sc_name, new_sc_desc)):
                            st.success("Added!")
                            st.rerun()

# ============================================================================
# PAGE 4: REPORTS & ANALYTICS
# ============================================================================
elif page == "Reports & Analytics":
    st.header("Reports & Analytics")
    
    entities_df = load_data("legal_entities")
    
    if not entities_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Entities", len(entities_df))
        with col2:
            st.metric("Active Entities", len(entities_df[entities_df['status'] == 'Active']))
        with col3:
            parent_count = len(entities_df[entities_df['parent_abn'].isna()])
            st.metric("Parent Entities", parent_count)
        
        st.markdown("---")
        
        # Entity breakdown
        st.subheader("Entity Breakdown by Type")
        type_breakdown = entities_df.groupby(['entity_type', 'status']).size().reset_index(name='count')
        st.dataframe(type_breakdown, use_container_width=True, hide_index=True)
        
        # Hierarchy summary
        st.subheader("Hierarchy Summary")
        hierarchy_data = []
        for _, entity in entities_df.iterrows():
            children_count = len(entities_df[entities_df['parent_abn'] == entity['abn']])
            hierarchy_data.append({
                'Entity': entity['entity_name'],
                'ABN': entity['abn'],
                'Type': entity['entity_type'],
                'Has Parent': 'Yes' if pd.notna(entity['parent_abn']) else 'No',
                'Children Count': children_count
            })
        
        hierarchy_df = pd.DataFrame(hierarchy_data)
        st.dataframe(hierarchy_df, use_container_width=True, hide_index=True)

# Footer
st.markdown("---")
st.caption("Legal Entity & Sector Management System | Built with Streamlit")
