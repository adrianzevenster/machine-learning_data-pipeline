USE RawData;

INSERT IGNORE INTO schema_migrations (version, description)
VALUES ('002', 'Add serving_predictions table for A/B outcome tracking');

CREATE TABLE IF NOT EXISTS serving_predictions (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    request_id          VARCHAR(64)  NOT NULL,
    msisdn              VARCHAR(32),
    model_name          VARCHAR(128) NOT NULL,
    model_stage         VARCHAR(32)  NOT NULL,
    model_variant       VARCHAR(16)  NOT NULL DEFAULT 'champion',
    prediction          TINYINT      NOT NULL,
    probability_churn   FLOAT        NOT NULL,
    probability_retain  FLOAT        NOT NULL,
    served_at           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actual_churn        TINYINT      NULL,
    outcome_recorded_at TIMESTAMP    NULL,
    INDEX idx_sp_msisdn    (msisdn),
    INDEX idx_sp_variant   (model_variant),
    INDEX idx_sp_served_at (served_at),
    INDEX idx_sp_outcome   (actual_churn)
);
