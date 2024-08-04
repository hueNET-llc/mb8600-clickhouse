-- PLEASE NOTE
-- The buffer tables are what I personally use to do buffer and batch inserts
-- You may have to modify them to work in your setup

CREATE TABLE docsis (
        modem_name LowCardinality(String), -- Modem name
        modem_config_filename LowCardinality(Nullable(String)), -- Modem config filename
        modem_uptime UInt32, -- Modem uptime
        modem_version LowCardinality(String), -- Modem version
        modem_model LowCardinality(String), -- Modem model
        downstream_channels Array(Nested( -- Array of downstream channels
            channel_id UInt8, -- Downstream channel ID
            frequency Float32, -- Downstream frequency
            modulation LowCardinality(String), -- Downstream modulation
            power Float32, -- Downstream power
            snr Float32, -- Downstream signal-to-noise ratio
            corrected_errors Int64, -- Downstream corrected errors
            uncorrected_errors Int64, -- Downstream uncorrected errors
            -- Some modems (MB8600) have overflow bugs so we need to store error counters signed
        )),
        upstream_channels Array(Nested( -- Array of upstream channels
            channel_id UInt8, -- Upstream channel ID
            frequency Float32, -- Upstream frequency
            modulation LowCardinality(String), -- Upstream modulation
            power Float32, -- Upstream power
            width Float32, -- Upstream width
        )),
        scrape_latency Float32, -- Modem scrape latency
        timestamp DateTime DEFAULT now() -- Data timestamp
) ENGINE = MergeTree() PARTITION BY toDate(timestamp) ORDER BY (modem_name, timestamp) PRIMARY KEY (modem_name, timestamp);

CREATE TABLE docsis_buffer (
        modem_name LowCardinality(String), -- Modem name
        modem_config_filename LowCardinality(Nullable(String)), -- Modem config filename
        modem_uptime UInt32, -- Modem uptime
        modem_version LowCardinality(String), -- Modem version
        modem_model LowCardinality(String), -- Modem model
        downstream_channels Array(Nested( -- Array of downstream channels
            channel_id UInt8, -- Downstream channel ID
            frequency Float32, -- Downstream frequency
            modulation LowCardinality(String), -- Downstream modulation
            power Float32, -- Downstream power
            snr Float32, -- Downstream signal-to-noise ratio
            corrected_errors Int64, -- Downstream corrected errors
            uncorrected_errors Int64, -- Downstream uncorrected errors
            -- Some modems (MB8600) have overflow bugs so we need to store error counters signed
        )),
        upstream_channels Array(Nested( -- Array of upstream channels
            channel_id UInt8, -- Upstream channel ID
            frequency Float32, -- Upstream frequency
            modulation LowCardinality(String), -- Upstream modulation
            power Float32, -- Upstream power
            width Float32, -- Upstream width
        )),
        scrape_latency Float32, -- Modem scrape latency
        timestamp DateTime DEFAULT now() -- Data timestamp
    ) ENGINE = Buffer(homelab, docsis, 1, 10, 10, 10, 100, 10000, 10000);
