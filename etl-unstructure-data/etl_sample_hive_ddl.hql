create database ginnie;
use ginnie;

create external table llmon1_file_header(
    file_header string, 
    file_name string,
    file_number smallint, 
    correction_flag string,
    file_date date,
    file_date_generated date) stored as orc;

create external table llmon1_pool(
    cusip string,
    pool_id string,
    issue_type string,
    pool_type string,
    pool_issue_date date,
    issuer_id string,
    as_of_date date) stored as orc;

create external table llmon1_loan(
    pool_id string,
    seqnum string,
    issuer_id string,
    agency string,
    loan_purpose string,
    refinance_type string,
    first_payment_date    date,
    maturity_date string,
    interest_rate string,
    opb_pool_issuance string,
    upb_pool_issuance string,
    upb_loan string,
    original_term smallint,
    loanage smallint,
    rem_loan_term smallint,
    month_delinquent smallint,
    month_prepaid smallint,
    loan_gross_margin string,
    loan_to_value string,
    combined_ltv string,
    total_debt_expense_ratio string,
    credit_score string,
    down_payment_assistance string,
    buy_down_status string,
    upfront_mip decimal(2,3),
    annual_mip decimal(2,3),
    borrower_count smallint,
    first_time_buyer string,
    property_type string,
    state string,
    msa int,
    third_party_origination_type string,
    curr_month_liquidation_flag string,
    removal_reason string,
    as_of_date date,
    loan_origination_date date,
    seller_issue_id string,
    index_type string,
    look_back_period smallint,
    interest_rate_change_date date,
    initial_int_rate_cap smallint,
    subsequent_int_rate_cap smallint,
    life_int_rate_cap smallint,
    nxt_int_rate_change_ceiling decimal(2,3),
    lifetime_int_rate_ceiling decimal(2,3),
    lifetime_int_rate_floor decimal(2,3),
    prospective_int_rate decimal(2,3)) stored as orc;

create external table llmon1_active_pool_summary(
    cusip string,
    pool_id string,
    issue_type string,
    pool_type string,
    pool_issue_date date,
    issuer_id string,
    as_of_date date,
    loan_cnt smallint) stored as orc;

create external table llmon1_file_trailer(
    file_name string,
    file_no string,
    pool_cnt smallint,
    loan_cnt smallint,
    total_record_cnt bigint,
    as_of_date date) stored as parquet;
