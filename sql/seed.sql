-- seed.sql — reference data required before write/read benchmarks.
-- Safe to re-run (ON CONFLICT DO NOTHING).

-- CommonBaseType seed
INSERT INTO "DM"."CommonBaseType" ("Id", "Title", "Icon", "CreationDate", "IsDeleted")
VALUES (1000, 'DeviceItemType', '', NOW(), FALSE)
ON CONFLICT ("Id") DO NOTHING;

-- CommonBaseValue seed (fuel types)
INSERT INTO "DM"."CommonBaseValue" ("Id", "CommonBaseTypeId", "Title", "Icon", "CreationDate", "IsDeleted")
VALUES
  (1000883,   1000, 'DieselFuel',             '', NOW(), FALSE),
  (1000884,   1000, 'Gasoline',               '', NOW(), FALSE),
  (10003296,  1000, 'Gasoline_CNG',            '', NOW(), FALSE),
  (10003323,  1000, 'Gasoline_CNG_Conversion', '', NOW(), FALSE),
  (10003325,  1000, 'Kerosene',               '', NOW(), FALSE),
  (10003377,  1000, 'Gasoline_Hybrid',         '', NOW(), FALSE)
ON CONFLICT ("Id") DO NOTHING;

-- Sequences (ensure next auto-generated IDs don't collide with manual ones)
SELECT setval(pg_get_serial_sequence('"DM"."CommonBaseType"', 'Id'),
    GREATEST((SELECT COALESCE(MAX("Id"), 0) FROM "DM"."CommonBaseType") + 1, 1), false);
SELECT setval(pg_get_serial_sequence('"DM"."CommonBaseValue"', 'Id'),
    GREATEST((SELECT COALESCE(MAX("Id"), 0) FROM "DM"."CommonBaseValue") + 1, 1), false);

-- Province + City + Address baseline (needed for write tests)
DO $$
DECLARE
    prov_id  integer;
    city_id  integer;
    addr_id  integer;
BEGIN
    IF (SELECT COUNT(*) FROM "DM"."Province") < 10 THEN
        FOR i IN 1..10 LOOP
            INSERT INTO "DM"."Province" ("Title", "CreationDate", "IsDeleted")
            VALUES ('Province_' || LPAD(i::text, 2, '0'), NOW(), FALSE)
            RETURNING "Id" INTO prov_id;

            FOR j IN 1..5 LOOP
                INSERT INTO "DM"."City" ("Title", "ProvinceId", "CreationDate", "IsDeleted")
                VALUES ('City_P' || i || '_C' || j, prov_id, NOW(), FALSE)
                RETURNING "Id" INTO city_id;
            END LOOP;

            INSERT INTO "DM"."Address" ("Detail","PostalCode","Mobile","Phone","Email","Fax","ProvinceId","CityId","CreationDate","IsDeleted")
            VALUES ('Addr_' || i, LPAD(i::text, 10, '0'), 9000000000 + i, '0211234567', 'seed@example.com', '0211234567', prov_id, city_id, NOW(), FALSE)
            RETURNING "Id" INTO addr_id;
        END LOOP;
    END IF;
END $$;

-- Manufacturer baseline
DO $$
DECLARE
    addr_id integer;
BEGIN
    SELECT "Id" INTO addr_id FROM "DM"."Address" LIMIT 1;
    IF addr_id IS NOT NULL AND (SELECT COUNT(*) FROM "DM"."Manufacturer") < 20 THEN
        FOR i IN 1..20 LOOP
            INSERT INTO "DM"."Manufacturer" ("Name", "AddressId", "CreationDate", "IsDeleted")
            VALUES ('Manufacturer_' || LPAD(i::text, 2, '0'), addr_id, NOW(), FALSE)
            ON CONFLICT DO NOTHING;
        END LOOP;
    END IF;
END $$;
