import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, date
import uuid

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
    
    # Reporting Groups table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reporting_groups (
            reporting_group_code TEXT PRIMARY KEY,
            reporting_group_name TEXT NOT NULL,
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
            reporting_group_code TEXT NOT NULL,
            sector_code TEXT NOT NULL,
            abn TEXT NOT NULL,
            effective_date DATE NOT NULL,
            end_date DATE,
            is_active BOOLEAN DEFAULT 1,
            created_by TEXT,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            modified_by TEXT,
            modified_date TIMESTAMP
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

# Insert sample data based on Alpha Holdings structure
def insert_sample_data():
    """Insert sample data for demo purposes"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Check if data exists
    cursor.execute("SELECT COUNT(*) FROM legal_entities")
    if cursor.fetchone()[0] == 0:
        # Insert reporting groups
        cursor.executemany("""
            INSERT INTO reporting_groups (reporting_group_code, reporting_group_name, description)
            VALUES (?, ?, ?)
        """, [
            ('FIN_INT', 'Financial Internal Reporting', 'Management and internal financial reporting'),
            ('FIN_REG', 'Financial Regulatory Reporting', 'Statutory and regulator submissions'),
            ('OPS_MIS', 'Operations MIS Reporting', 'Operational performance reporting')
        ])
        
        # Insert sector codes
        cursor.executemany("""
            INSERT INTO sector_codes (sector_code, sector_name, sector_description)
            VALUES (?, ?, ?)
        """, [
            ('F1N01', 'Financial Services', 'Banking, lending, and financial operations'),
            ('T3C02', 'Technology', 'Software, IT services, and infrastructure'),
            ('O9P88', 'Operations', 'Logistics, supply chain, and operations'),
            ('R7D55', 'Research & Dev', 'Innovation and product development')
        ])
        
        # Insert legal entities
        cursor.executemany("""
            INSERT INTO legal_entities (abn, entity_name, parent_abn, entity_type, status, effective_date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            ('91000000001', 'Alpha Holdings Pty Ltd', None, 'Parent', 'Active', '2010-01-01'),
            ('91000000002', 'Alpha Finance Pty Ltd', '91000000001', 'Subsidiary', 'Active', '2012-03-15'),
            ('91000000003', 'Alpha Operations JV', '91000000001', 'JV', 'Active', '2013-06-01'),
            ('91000000004', 'Alpha Technology Pty Ltd', '91000000001', 'Subsidiary', 'Active', '2014-09-20'),
            ('91000000005', 'Alpha Finance Services Pty Ltd', '91000000002', 'Subsidiary', 'Active', '2015-02-01'),
            ('91000000006', 'Alpha Finance Consulting JV', '91000000002', 'JV', 'Active', '2016-05-10'),
            ('91000000007', 'Alpha Ops Logistics Pty Ltd', '91000000003', 'Subsidiary', 'Active', '2016-08-01'),
            ('91000000008', 'Alpha Ops Support JV', '91000000003', 'JV', 'Active', '2017-01-15'),
            ('91000000009', 'Alpha Tech Software Pty Ltd', '91000000004', 'Subsidiary', 'Active', '2017-07-01'),
            ('91000000010', 'Alpha Tech Infrastructure JV', '91000000004', 'JV', 'Active', '2018-03-12'),
            ('91000000011', 'Alpha Tech R&D Pty Ltd', '91000000004', 'Subsidiary', 'Active', '2019-11-05')
        ])
        
        # Insert sector mappings
        cursor.executemany("""
            INSERT INTO sector_abn_mapping 
            (mapping_id, reporting_group_code, sector_code, abn, effective_date)
            VALUES (?, ?, ?, ?, ?)
        """, [
            ('MAP001', 'FIN_INT', 'F1N01', '91000000002', '2020-01-01'),
            ('MAP002', 'FIN_REG', 'F1N01', '91000000005', '2020-01-01'),
            ('MAP003', 'FIN_INT', 'T3C02', '91000000004', '2020-01-01'),
            ('MAP004', 'FIN_REG', 'T3C02', '91000000009', '2020-01-01'),
            ('MAP005', 'OPS_MIS', 'O9P88', '91000000003', '2020-01-01'),
            ('MAP006', 'FIN_INT', 'O9P88', '91000000007', '2020-01-01'),
            ('MAP007', 'FIN_INT', 'R7D55', '91000000011', '2021-01-01'),
            ('MAP008', 'FIN_REG', 'R7D55', '91000000010', '2021-01-01')
        ])
        
        conn.commit()
    
    conn.close()

# Insert sample data
insert_sample_data()

# App UI
st.title("🏢 Legal Entity & Sector Code Management")
st.markdown("**Alpha Holdings Group** - Entity Hierarchy & Sector Mapping System")
st.markdown("---")

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Select Module",
    ["Dashboard", "Legal Entity Hierarchy", "Sector Code Mappings", "Reference Data", "Reports & Analytics"]
)

# Reset database button
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Reset to Sample Data"):
    import os
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    st.sidebar.success("Database reset! Refreshing...")
    st.rerun()

# ============================================================================
# PAGE 0: DASHBOARD
# ============================================================================
if page == "Dashboard":
    st.header("📊 System Dashboard")
    
    entities_df = load_data("legal_entities")
    mappings_df = load_data("sector_abn_mapping")
    sectors_df = load_data("sector_codes")
    groups_df = load_data("reporting_groups")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Entities", len(entities_df))
    with col2:
        active_count = len(entities_df[entities_df['status'] == 'Active'])
        st.metric("Active Entities", active_count)
    with col3:
        st.metric("Sector Mappings", len(mappings_df[mappings_df['is_active'] == 1]))
    with col4:
        st.metric("Reporting Groups", len(groups_df[groups_df['is_active'] == 1]))
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Entity Distribution by Type")
        if not entities_df.empty:
            type_dist = entities_df['entity_type'].value_counts().reset_index()
            type_dist.columns = ['Entity Type', 'Count']
            st.dataframe(type_dist, use_container_width=True, hide_index=True)
    
    with col2:
        st.subheader("Sector Coverage")
        if not sectors_df.empty:
            st.dataframe(
                sectors_df[['sector_code', 'sector_name']],
                use_container_width=True,
                hide_index=True
            )
    
    st.markdown("---")
    st.subheader("Alpha Holdings Group Structure Overview")
    st.info("""
    **Parent Entity:** Alpha Holdings Pty Ltd (91000000001)
    
    **Direct Subsidiaries:**
    - Alpha Finance Pty Ltd (Finance Division)
    - Alpha Operations JV (Operations Division)
    - Alpha Technology Pty Ltd (Technology Division)
    
    **Total Entities:** 11 legal entities across 4 sectors
    """)

# ============================================================================
# PAGE 1: LEGAL ENTITY HIERARCHY
# ============================================================================
elif page == "Legal Entity Hierarchy":
    st.header("Legal Entity Hierarchy Management")
    
    tab1, tab2, tab3 = st.tabs(["View Hierarchy", "Add Entity", "Edit/Delete Entity"])
    
    with tab1:
        st.subheader("Current Legal Entity Hierarchy")
        
        entities_df = load_data("legal_entities")
        
        if not entities_df.empty:
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
            
            st.subheader("📊 Hierarchy Tree View")
            
            def build_tree(parent_abn, level=0):
                children = filtered_df[filtered_df['parent_abn'] == parent_abn].sort_values('entity_name')
                for idx, child in children.iterrows():
                    indent = "    " * level
                    connector = "└── " if level > 0 else ""
                    type_emoji = "🏛️" if child['entity_type'] == 'Parent' else "🏢" if child['entity_type'] == 'Subsidiary' else "🤝"
                    status_emoji = "✅" if child['status'] == 'Active' else "⏸️"
                    st.text(f"{indent}{connector}{type_emoji} {child['entity_name']} ({child['abn']}) - {child['entity_type']} {status_emoji}")
                    build_tree(child['abn'], level + 1)
            
            parent_entities = filtered_df[filtered_df['parent_abn'].isna()]
            for _, parent in parent_entities.iterrows():
                build_tree(None, 0)
        else:
            st.info("No legal entities found.")
    
    with tab2:
        st.subheader("Add New Legal Entity")
        
        with st.form("add_entity_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                new_abn = st.text_input("ABN *", max_chars=11)
                new_entity_name = st.text_input("Entity Name *")
                new_entity_type = st.selectbox("Entity Type *", ["Parent", "Subsidiary", "JV", "Branch", "Other"])
            
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
                    if not entities_df.empty and new_abn in entities_df['abn'].values:
                        st.error(f"ABN {new_abn} already exists!")
                    else:
                        query = """
                            INSERT INTO legal_entities 
                            (abn, entity_name, parent_abn, entity_type, status, effective_date, created_by)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """
                        params = (new_abn, new_entity_name, None if new_parent_abn == "None" else new_parent_abn,
                                new_entity_type, new_status, new_effective_date, 'streamlit_user')
                        
                        if execute_query(query, params):
                            st.success(f"✅ Successfully added entity: {new_entity_name} ({new_abn})")
                            st.rerun()
    
    with tab3:
        st.subheader("Edit or Delete Legal Entity")
        
        entities_df = load_data("legal_entities")
        
        if not entities_df.empty:
            selected_abn = st.selectbox(
                "Select Entity",
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
                            entity_types = ["Parent", "Subsidiary", "JV", "Branch", "Other"]
                            current_type_idx = entity_types.index(entity_data['entity_type']) if entity_data['entity_type'] in entity_types else 0
                            edit_type = st.selectbox("Entity Type", entity_types, index=current_type_idx)
                        
                        with edit_col2:
                            statuses = ["Active", "Inactive", "Pending"]
                            current_status_idx = statuses.index(entity_data['status']) if entity_data['status'] in statuses else 0
                            edit_status = st.selectbox("Status", statuses, index=current_status_idx)
                        
                        update_submitted = st.form_submit_button("Update Entity")
                        
                        if update_submitted:
                            query = """
                                UPDATE legal_entities 
                                SET entity_name = ?, entity_type = ?, status = ?, modified_by = ?, modified_date = ?
                                WHERE abn = ?
                            """
                            params = (edit_name, edit_type, edit_status, 'streamlit_user', datetime.now(), selected_abn)
                            
                            if execute_query(query, params):
                                st.success("✅ Entity updated successfully!")
                                st.rerun()
                
                with col2:
                    st.write("")
                    st.write("")
                    if st.button("🗑️ Delete", type="secondary", use_container_width=True):
                        children = entities_df[entities_df['parent_abn'] == selected_abn]
                        if not children.empty:
                            st.error("❌ Cannot delete entity with children!")
                        else:
                            if delete_record("legal_entities", "abn", selected_abn):
                                st.success("✅ Deleted successfully!")
                                st.rerun()

# ============================================================================
# PAGE 2: SECTOR CODE MAPPINGS
# ============================================================================
elif page == "Sector Code Mappings":
    st.header("Sector Code to ABN Mapping")
    
    tab1, tab2, tab3 = st.tabs(["View Mappings", "Add Mapping", "Edit/Delete Mapping"])
    
    with tab1:
        st.subheader("Current Sector Code Mappings")
        
        conn = sqlite3.connect(DB_FILE)
        query = """
            SELECT 
                m.mapping_id, m.reporting_group_code, g.reporting_group_name,
                m.sector_code, s.sector_name, m.abn, e.entity_name,
                m.effective_date, m.end_date, m.is_active
            FROM sector_abn_mapping m
            LEFT JOIN reporting_groups g ON m.reporting_group_code = g.reporting_group_code
            LEFT JOIN sector_codes s ON m.sector_code = s.sector_code
            LEFT JOIN legal_entities e ON m.abn = e.abn
            ORDER BY g.reporting_group_name, s.sector_name
        """
        mappings_df = pd.read_sql_query(query, conn)
        conn.close()
        
        if not mappings_df.empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                group_filter = st.multiselect(
                    "Filter by Reporting Group",
                    options=mappings_df['reporting_group_name'].unique(),
                    default=mappings_df['reporting_group_name'].unique()
                )
            
            with col2:
                sector_filter = st.multiselect(
                    "Filter by Sector",
                    options=mappings_df['sector_name'].unique(),
                    default=mappings_df['sector_name'].unique()
                )
            
            with col3:
                active_only = st.checkbox("Show Active Only", value=True)
            
            filtered = mappings_df[
                (mappings_df['reporting_group_name'].isin(group_filter)) &
                (mappings_df['sector_name'].isin(sector_filter))
            ]
            
            if active_only:
                filtered = filtered[filtered['is_active'] == 1]
            
            st.dataframe(
                filtered[['reporting_group_name', 'sector_name', 'entity_name', 'abn', 'effective_date', 'is_active']],
                use_container_width=True,
                hide_index=True
            )
            
            st.markdown("---")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Mappings", len(filtered))
            with col2:
                st.metric("Active Mappings", len(filtered[filtered['is_active'] == 1]))
            with col3:
                st.metric("Unique Entities", filtered['abn'].nunique())
        else:
            st.info("No mappings found.")
    
    with tab2:
        st.subheader("Add New Sector Code Mapping")
        
        groups_df = load_data("reporting_groups")
        sector_codes_df = load_data("sector_codes")
        entities_df = load_data("legal_entities")
        
        if not groups_df.empty and not sector_codes_df.empty and not entities_df.empty:
            with st.form("add_mapping_form"):
                col1, col2 = st.columns(2)
                
                with col1:
                    mapping_group = st.selectbox(
                        "Reporting Group *",
                        groups_df['reporting_group_code'].tolist(),
                        format_func=lambda x: groups_df[groups_df['reporting_group_code']==x]['reporting_group_name'].values[0]
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
                        (mapping_id, reporting_group_code, sector_code, abn, effective_date, created_by)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """
                    params = (mapping_id, mapping_group, mapping_sector, mapping_abn, mapping_effective_date, 'streamlit_user')
                    
                    if execute_query(query, params):
                        st.success("✅ Mapping added successfully!")
                        st.rerun()
        else:
            st.warning("⚠️ Please configure Reference Data first.")
    
    with tab3:
        st.subheader("Edit or Delete Mapping")
        
        conn = sqlite3.connect(DB_FILE)
        query = """
            SELECT 
                m.mapping_id, g.reporting_group_name, s.sector_name, e.entity_name,
                m.effective_date, m.end_date, m.is_active
            FROM sector_abn_mapping m
            LEFT JOIN reporting_groups g ON m.reporting_group_code = g.reporting_group_code
            LEFT JOIN sector_codes s ON m.sector_code = s.sector_code
            LEFT JOIN legal_entities e ON m.abn = e.abn
        """
        mappings_df = pd.read_sql_query(query, conn)
        conn.close()
        
        if not mappings_df.empty:
            selected_mapping = st.selectbox(
                "Select Mapping",
                mappings_df['mapping_id'].tolist(),
                format_func=lambda x: f"{mappings_df[mappings_df['mapping_id']==x]['reporting_group_name'].values[0]} | {mappings_df[mappings_df['mapping_id']==x]['sector_name'].values[0]} → {mappings_df[mappings_df['mapping_id']==x]['entity_name'].values[0]}"
            )
            
            if selected_mapping:
                mapping_data = mappings_df[mappings_df['mapping_id'] == selected_mapping].iloc[0]
                
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    with st.form("edit_mapping_form"):
                        st.write("**Edit Mapping**")
                        
                        edit_active = st.checkbox("Active", value=bool(mapping_data['is_active']))
                        edit_end_date = st.date_input(
                            "End Date (Optional)", 
                            value=pd.to_datetime(mapping_data['end_date']).date() if pd.notna(mapping_data['end_date']) else None
                        )
                        
                        update_submitted = st.form_submit_button("Update")
                        
                        if update_submitted:
                            query = """
                                UPDATE sector_abn_mapping 
                                SET is_active = ?, end_date = ?, modified_by = ?, modified_date = ?
                                WHERE mapping_id = ?
                            """
                            params = (1 if edit_active else 0, edit_end_date, 'streamlit_user', datetime.now(), selected_mapping)
                            
                            if execute_query(query, params):
                                st.success("✅ Updated successfully!")
                                st.rerun()
                
                with col2:
                    st.write("")
                    st.write("")
                    if st.button("🗑️ Delete", type="secondary", use_container_width=True):
                        if delete_record("sector_abn_mapping", "mapping_id", selected_mapping):
                            st.success("✅ Deleted!")
                            st.rerun()

# ============================================================================
# PAGE 3: REFERENCE DATA
# ============================================================================
elif page == "Reference Data":
    st.header("Reference Data Management")
    
    tab1, tab2 = st.tabs(["Reporting Groups", "Sector Codes"])
    
    with tab1:
        st.subheader("Reporting Groups")
        
        groups_df = load_data("reporting_groups")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            if not groups_df.empty:
                st.dataframe(
                    groups_df[['reporting_group_code', 'reporting_group_name', 'description', 'is_active']],
                    use_container_width=True,
                    hide_index=True
                )
        
        with col2:
            with st.form("add_reporting_group"):
                st.write("**Add New**")
                new_code = st.text_input("Code", max_chars=20)
                new_name = st.text_input("Name")
                new_desc = st.text_area("Description")
                
                if st.form_submit_button("Add"):
                    if new_code and new_name:
                        query = "INSERT INTO reporting_groups (reporting_group_code, reporting_group_name, description) VALUES (?, ?, ?)"
                        if execute_query(query, (new_code, new_name, new_desc)):
                            st.success("✅ Added!")
                            st.rerun()
                    else:
                        st.error("Fill required fields")
    
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
                new_code = st.text_input("Code", max_chars=10)
                new_name = st.text_input("Name")
                new_desc = st.text_area("Description")
                
                if st.form_submit_button("Add"):
                    if new_code and new_name:
                        query = "INSERT INTO sector_codes (sector_code, sector_name, sector_description) VALUES (?, ?, ?)"
                        if execute_query(query, (new_code, new_name, new_desc)):
                            st.success("✅ Added!")
                            st.rerun()
                    else:
                        st.error("Fill required fields")

# ============================================================================
# PAGE 4: REPORTS & ANALYTICS
# ============================================================================
elif page == "Reports & Analytics":
    st.header("Reports & Analytics")
    
    entities_df = load_data("legal_entities")
    
    if not entities_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Entities", len(entities_df))
        with col2:
            st.metric("Active Entities", len(entities_df[entities_df['status'] == 'Active']))
        with col3:
            st.metric("Parent Entities", len(entities_df[entities_df['parent_abn'].isna()]))
        with col4:
            st.metric("Joint Ventures", len(entities_df[entities_df['entity_type'] == 'JV']))
            st.metric("Joint Ventures", jv_count)
        
        st.markdown("---")
        
        # Report Selection
        report_type = st.selectbox(
            "Select Report Type",
            ["Entity Summary", "Hierarchy Breakdown", "Sector Mapping Summary", "Detailed Entity Report"]
        )
        
        if report_type == "Entity Summary":
            st.subheader("Entity Summary Report")
            
            summary = entities_df.groupby(['entity_type', 'status']).size().reset_index(name='count')
            st.dataframe(summary, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            st.subheader("Entity Creation Timeline")
            timeline_df = entities_df.sort_values('effective_date')[['entity_name', 'entity_type', 'effective_date', 'status']]
            st.dataframe(timeline_df, use_container_width=True, hide_index=True)
        
        elif report_type == "Hierarchy Breakdown":
            st.subheader("Hierarchy Breakdown")
            
            hierarchy_data = []
            for _, entity in entities_df.iterrows():
                children_count = len(entities_df[entities_df['parent_abn'] == entity['abn']])
                
                hierarchy_data.append({
                    'ABN': entity['abn'],
                    'Entity Name': entity['entity_name'],
                    'Type': entity['entity_type'],
                    'Has Parent': 'Yes' if pd.notna(entity['parent_abn']) else 'No',
                    'Children Count': children_count,
                    'Status': entity['status']
                })
            
            hierarchy_df = pd.DataFrame(hierarchy_data)
            st.dataframe(hierarchy_df, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            st.subheader("Visual Hierarchy")
            
            def display_hierarchy(parent_abn, level=0):
                children = entities_df[entities_df['parent_abn'] == parent_abn].sort_values('entity_name')
                for _, child in children.iterrows():
                    indent = "    " * level
                    connector = "└── " if level > 0 else ""
                    st.text(f"{indent}{connector}{child['entity_name']} ({child['abn']}) - {child['entity_type']}")
                    display_hierarchy(child['abn'], level + 1)
            
            root_entities = entities_df[entities_df['parent_abn'].isna()]
            for _, root in root_entities.iterrows():
                display_hierarchy(None, 0)
        
        elif report_type == "Sector Mapping Summary":
            st.subheader("Sector Mapping Summary")
            
            conn = sqlite3.connect(DB_FILE)
            query = """
                SELECT 
                    g.reporting_group_name,
                    s.sector_name,
                    COUNT(m.mapping_id) as mapping_count,
                    COUNT(DISTINCT m.abn) as unique_entities
                FROM sector_abn_mapping m
                LEFT JOIN reporting_groups g ON m.reporting_group_code = g.reporting_group_code
                LEFT JOIN sector_codes s ON m.sector_code = s.sector_code
                WHERE m.is_active = 1
                GROUP BY g.reporting_group_name, s.sector_name
                ORDER BY g.reporting_group_name, s.sector_name
            """
            summary_df = pd.read_sql_query(query, conn)
            conn.close()
            
            st.dataframe(summary_df, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            st.subheader("Detailed Sector Mappings")
            
            conn = sqlite3.connect(DB_FILE)
            detail_query = """
                SELECT 
                    g.reporting_group_name as "Reporting Group",
                    s.sector_name as "Sector",
                    e.entity_name as "Entity",
                    e.abn as "ABN",
                    e.entity_type as "Entity Type",
                    m.effective_date as "Effective Date"
                FROM sector_abn_mapping m
                LEFT JOIN reporting_groups g ON m.reporting_group_code = g.reporting_group_code
                LEFT JOIN sector_codes s ON m.sector_code = s.sector_code
                LEFT JOIN legal_entities e ON m.abn = e.abn
                WHERE m.is_active = 1
                ORDER BY g.reporting_group_name, s.sector_name, e.entity_name
            """
            detail_df = pd.read_sql_query(detail_query, conn)
            conn.close()
            
            st.dataframe(detail_df, use_container_width=True, hide_index=True)
        
        elif report_type == "Detailed Entity Report":
            st.subheader("Detailed Entity Report")
            
            selected_abn = st.selectbox(
                "Select Entity",
                entities_df['abn'].tolist(),
                format_func=lambda x: f"{entities_df[entities_df['abn']==x]['entity_name'].values[0]} ({x})"
            )
            
            if selected_abn:
                entity = entities_df[entities_df['abn'] == selected_abn].iloc[0]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("### Basic Information")
                    st.write(f"**ABN:** {entity['abn']}")
                    st.write(f"**Name:** {entity['entity_name']}")
                    st.write(f"**Type:** {entity['entity_type']}")
                    st.write(f"**Status:** {entity['status']}")
                    st.write(f"**Effective Date:** {entity['effective_date']}")
                
                with col2:
                    st.markdown("### Hierarchy Information")
                    if pd.notna(entity['parent_abn']):
                        parent = entities_df[entities_df['abn'] == entity['parent_abn']].iloc[0]
                        st.write(f"**Parent Entity:** {parent['entity_name']}")
                        st.write(f"**Parent ABN:** {parent['abn']}")
                    else:
                        st.write("**Parent Entity:** None (Root Entity)")
                    
                    children_count = len(entities_df[entities_df['parent_abn'] == selected_abn])
                    st.write(f"**Child Entities:** {children_count}")
                
                st.markdown("---")
                st.markdown("### Child Entities")
                children = entities_df[entities_df['parent_abn'] == selected_abn]
                if not children.empty:
                    st.dataframe(
                        children[['abn', 'entity_name', 'entity_type', 'status', 'effective_date']],
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.info("No child entities")
                
                st.markdown("---")
                st.markdown("### Sector Mappings")
                
                conn = sqlite3.connect(DB_FILE)
                mapping_query = """
                    SELECT 
                        g.reporting_group_name as "Reporting Group",
                        s.sector_name as "Sector",
                        m.effective_date as "Effective Date",
                        m.is_active as "Active"
                    FROM sector_abn_mapping m
                    LEFT JOIN reporting_groups g ON m.reporting_group_code = g.reporting_group_code
                    LEFT JOIN sector_codes s ON m.sector_code = s.sector_code
                    WHERE m.abn = ?
                    ORDER BY g.reporting_group_name
                """
                mappings = pd.read_sql_query(mapping_query, conn, params=(selected_abn,))
                conn.close()
                
                if not mappings.empty:
                    st.dataframe(mappings, use_container_width=True, hide_index=True)
                else:
                    st.info("No sector mappings for this entity")
        
        st.markdown("---")
        st.subheader("📥 Export Data")
        
        export_option = st.selectbox(
            "Select data to export",
            ["All Entities", "All Sector Mappings", "Complete Report"]
        )
        
        if st.button("Generate CSV Export"):
            if export_option == "All Entities":
                csv = entities_df.to_csv(index=False)
                st.download_button(
                    label="Download Entities CSV",
                    data=csv,
                    file_name="alpha_holdings_entities.csv",
                    mime="text/csv"
                )
            elif export_option == "All Sector Mappings":
                conn = sqlite3.connect(DB_FILE)
                query = """
                    SELECT 
                        m.*,
                        g.reporting_group_name,
                        s.sector_name,
                        e.entity_name
                    FROM sector_abn_mapping m
                    LEFT JOIN reporting_groups g ON m.reporting_group_code = g.reporting_group_code
                    LEFT JOIN sector_codes s ON m.sector_code = s.sector_code
                    LEFT JOIN legal_entities e ON m.abn = e.abn
                """
                mappings_df = pd.read_sql_query(query, conn)
                conn.close()
                
                csv = mappings_df.to_csv(index=False)
                st.download_button(
                    label="Download Mappings CSV",
                    data=csv,
                    file_name="alpha_holdings_sector_mappings.csv",
                    mime="text/csv"
                )
    else:
        st.info("No data available for reporting.")

# Footer
st.markdown("---")
st.caption("Legal Entity & Sector Management System | Built with Streamlit")

