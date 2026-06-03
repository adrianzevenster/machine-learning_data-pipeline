CREATE DATABASE IF NOT EXISTS RawData;

USE RawData;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(32) PRIMARY KEY,
    description VARCHAR(255) NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS DP_CDR_Data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    DP_DATE DATETIME NOT NULL,
    DP_MSISDN VARCHAR(64) NOT NULL,
    DP_MOC_COUNT INT NOT NULL DEFAULT 0,
    DP_MOC_DURATION DOUBLE NOT NULL DEFAULT 0,
    DP_MTC_COUNT INT NOT NULL DEFAULT 0,
    DP_MTC_DURATION DOUBLE NOT NULL DEFAULT 0,
    DP_MOSMS_COUNT INT NOT NULL DEFAULT 0,
    DP_MTSMS_COUNT INT NOT NULL DEFAULT 0,
    DP_DATA_COUNT INT NOT NULL DEFAULT 0,
    DP_DATA_VOLUME DOUBLE NOT NULL DEFAULT 0,
    PSEUDO_CHURNED INT NOT NULL,
    INDEX idx_raw_date (DP_DATE),
    INDEX idx_raw_msisdn_date (DP_MSISDN, DP_DATE),
    INDEX idx_raw_label (PSEUDO_CHURNED)
);

CREATE TABLE IF NOT EXISTS Processed_Data (
    Date DATE NOT NULL,
    User VARCHAR(64) NOT NULL,
    M_Out_Call_Count INT NOT NULL DEFAULT 0,
    M_Out_Call_Time DOUBLE NOT NULL DEFAULT 0,
    M_Data_Sum DOUBLE NOT NULL DEFAULT 0,
    M_Data_Count INT NOT NULL DEFAULT 0,
    M_In_Call_Count INT NOT NULL DEFAULT 0,
    M_In_Call_Time DOUBLE NOT NULL DEFAULT 0,
    M_TENURE_CHURN INT NOT NULL,
    INDEX idx_processed_date (Date),
    INDEX idx_processed_label (M_TENURE_CHURN),
    INDEX idx_processed_user_date (User, Date)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(250) PRIMARY KEY,
    dag_id VARCHAR(250),
    git_sha VARCHAR(64),
    image_tag VARCHAR(250),
    data_start DATE,
    data_end DATE,
    raw_row_count BIGINT,
    processed_row_count BIGINT,
    prediction_row_count BIGINT,
    status VARCHAR(32) NOT NULL DEFAULT 'started',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS model_versions (
    model_version_id VARCHAR(250) PRIMARY KEY,
    run_id VARCHAR(250) NOT NULL,
    model_name VARCHAR(128) NOT NULL,
    algorithm VARCHAR(128) NOT NULL,
    parameters_json TEXT,
    metrics_json TEXT,
    artifact_uri VARCHAR(512),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_model_versions_run (run_id)
);

CREATE TABLE IF NOT EXISTS model_predictions (
    pipeline_run_id VARCHAR(250) NOT NULL,
    model_version_id VARCHAR(250) NOT NULL,
    label DOUBLE NOT NULL,
    prediction DOUBLE NOT NULL,
    probability_0 DOUBLE NOT NULL,
    probability_1 DOUBLE NOT NULL,
    Date DATETIME NOT NULL,
    INDEX idx_predictions_run (pipeline_run_id),
    INDEX idx_predictions_model_version (model_version_id),
    INDEX idx_predictions_date (Date),
    INDEX idx_predictions_label (label)
);

CREATE TABLE IF NOT EXISTS monitoring_reports (
    report_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    pipeline_run_id VARCHAR(250) NOT NULL,
    model_version_id VARCHAR(250),
    reference_rows BIGINT NOT NULL,
    analysis_rows BIGINT NOT NULL,
    metrics_path VARCHAR(512),
    plot_path VARCHAR(512),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_monitoring_reports_run (pipeline_run_id),
    INDEX idx_monitoring_reports_model_version (model_version_id)
);

INSERT IGNORE INTO schema_migrations (version, description)
VALUES ('001', 'create core pipeline tables');
