CREATE TABLE IF NOT EXISTS audit.dq_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      VARCHAR(36),
    run_ts      TIMESTAMP DEFAULT NOW(),
    layer       VARCHAR(20),
    table_name  VARCHAR(100),
    check_name  VARCHAR(100),
    status      VARCHAR(10),
    expected    VARCHAR(100),
    actual      VARCHAR(100),
    details     VARCHAR(255)
);