-- Table: Companies
CREATE TABLE companies (
    id TEXT PRIMARY KEY, -- Unique identifier for the company
    name TEXT NOT NULL UNIQUE, -- Company name
    domain TEXT NOT NULL UNIQUE, -- Company website domain
    industry TEXT, -- Industry category
    size TEXT, -- Size category (e.g., 11-50, 1000+)
    country TEXT, -- Country where the company is located
    created_date DATETIME NOT NULL, -- Record creation timestamp
    is_customer BOOLEAN DEFAULT 0, -- Whether the company is a customer
    annual_revenue REAL, -- Annual revenue in numeric format
    etl_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP -- Metadata for record creation
);

-- Table: Contacts
CREATE TABLE contacts (
    id TEXT PRIMARY KEY, -- Unique identifier for the contact
    first_name TEXT NOT NULL, -- First name
    last_name TEXT NOT NULL, -- Last name
    email TEXT NOT NULL UNIQUE, -- Email address
    phone TEXT UNIQUE, -- Phone number
    title TEXT, -- Job title
    status TEXT DEFAULT 'Unknown', -- Contact status (e.g., Lead, Customer, Churned)
    company_id TEXT NOT NULL, -- Reference to the company
    created_date DATETIME NOT NULL, -- Record creation timestamp
    last_modified DATETIME, -- Last modified timestamp
    etl_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, -- Metadata for record creation
    FOREIGN KEY (company_id) REFERENCES companies (id) ON DELETE SET NULL
);

-- Table: Opportunities
CREATE TABLE opportunities (
    id TEXT PRIMARY KEY, -- Unique identifier for the opportunity
    name TEXT NOT NULL, -- Name of the opportunity
    contact_id TEXT NOT NULL, -- Reference to the contact
    company_id TEXT NOT NULL, -- Reference to the company   
    amount REAL, -- Monetary amount of the opportunity 
    stage TEXT NOT NULL, -- Sales pipeline stage (e.g., Prospecting, Negotiation)
    product TEXT, -- Product or service related to the opportunity
    probability REAL, -- Probability of winning the opportunity
    created_date DATETIME NOT NULL, -- Record creation timestamp
    close_date DATE, -- Expected close date
    is_closed BOOLEAN DEFAULT 0, -- Whether the opportunity is closed
    forecast_category TEXT, -- Forecast category (e.g., Best Case, Commit)
    etl_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, -- Metadata for record creation
    FOREIGN KEY (company_id) REFERENCES companies (id) ON DELETE CASCADE,
    FOREIGN KEY (contact_id) REFERENCES contacts (id) ON DELETE CASCADE
);

-- Table: Activities
CREATE TABLE activities (
    id TEXT PRIMARY KEY, -- Unique identifier for the activity
    contact_id TEXT, -- Reference to the contact
    opportunity_id TEXT, -- Reference to the opportunity      
    type TEXT NOT NULL, -- Type of activity (e.g., Call, Email, Meeting, Task)
    subject TEXT NOT NULL, -- Brief description of the activity
    timestamp DATETIME NOT NULL, -- When the activity occurred
    duration_minutes INTEGER, -- Duration of the activity in minutes
    outcome TEXT, -- Result of the activity (e.g., Completed, No Show)
    notes TEXT, -- Additional notes
    etl_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, -- Metadata for record creation
    FOREIGN KEY (contact_id) REFERENCES contacts (id) ON DELETE SET NULL,
    FOREIGN KEY (opportunity_id) REFERENCES opportunities (id) ON DELETE SET NULL
);
