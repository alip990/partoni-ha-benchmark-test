-- write_heavy.sql — chain of 5 table inserts in a single CTE statement.
-- Used by WriteHeavyUser. No Python parameters — all values generated in SQL.
-- Tests multi-row WAL write throughput and replication lag under load.
WITH new_province AS (
    INSERT INTO "DM"."Province" ("Title", "CreationDate", "IsDeleted")
    VALUES ('Prov_' || SUBSTRING(MD5(RANDOM()::TEXT), 1, 6), NOW(), FALSE)
    RETURNING "Id"
),
new_city AS (
    INSERT INTO "DM"."City" ("Title", "ProvinceId", "CreationDate", "IsDeleted")
    SELECT 'City_' || SUBSTRING(MD5(RANDOM()::TEXT), 1, 6), "Id", NOW(), FALSE
    FROM new_province
    RETURNING "Id", "ProvinceId"
),
new_address AS (
    INSERT INTO "DM"."Address"
        ("Detail", "PostalCode", "Mobile", "Phone", "Email", "Fax",
         "ProvinceId", "CityId", "CreationDate", "IsDeleted")
    SELECT
        'Addr_' || SUBSTRING(MD5(RANDOM()::TEXT), 1, 8),
        LPAD(FLOOR(RANDOM() * 9999999999)::BIGINT::TEXT, 10, '0'),
        (9000000000 + FLOOR(RANDOM() * 999999999))::BIGINT,
        '021' || LPAD(FLOOR(RANDOM() * 99999999)::TEXT, 8, '0'),
        'bench_' || SUBSTRING(MD5(RANDOM()::TEXT), 1, 8) || '@example.com',
        '02100000000',
        nc."ProvinceId", nc."Id", NOW(), FALSE
    FROM new_city nc
    RETURNING "Id"
),
new_manufacturer AS (
    INSERT INTO "DM"."Manufacturer" ("Name", "AddressId", "CreationDate", "IsDeleted")
    SELECT 'Manu_' || SUBSTRING(MD5(RANDOM()::TEXT), 1, 6), "Id", NOW(), FALSE
    FROM new_address
    RETURNING "Id"
),
new_device AS (
    INSERT INTO "DM"."Device" ("DeviceStatus", "Activated", "CreationDate", "IsDeleted")
    VALUES ((FLOOR(RANDOM() * 5) + 1)::integer, TRUE, NOW(), FALSE)
    RETURNING "Id"
)
SELECT
    nd."Id"  AS device_id,
    nm."Id"  AS manufacturer_id
FROM new_device nd
CROSS JOIN new_manufacturer nm;
