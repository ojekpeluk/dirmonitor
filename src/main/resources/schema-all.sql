DROP TABLE domains IF EXISTS;

CREATE TABLE domains  (
    id BIGINT IDENTITY NOT NULL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    src_ip VARCHAR(20) NOT NULL,
    src_port INTEGER NOT NULL,
    dst_ip VARCHAR(20) NOT NULL,
    dst_port INTEGER NOT NULL,
    domain VARCHAR(100) NOT NULL,
    -- To ensure records are unique
    UNIQUE (timestamp, src_ip, domain)
);