-- read_heavy.sql — complex 8-table join + window aggregate.
-- Used by ReadHeavyUser. No parameters required (sampling via RANDOM()).
-- Exercises the read-replica load balancing in PgPool / CNPG.
SELECT
    d."Id"                          AS device_id,
    d."DeviceStatus",
    d."Activated",
    di."SerialNo",
    di."OsVersion",
    di."FirmwareVersion",
    m."Name"                        AS manufacturer,
    r."Name"                        AS representation,
    p."Title"                       AS province,
    c."Title"                       AS city,
    COUNT(sc."Id") OVER (PARTITION BY d."Id") AS sim_count,
    COUNT(dih."Id") OVER (PARTITION BY d."Id") AS history_count
FROM "DM"."Device" d
LEFT JOIN "DM"."DeviceItem"    di  ON di."DeviceId"        = d."Id"   AND di."IsDeleted"  = FALSE
LEFT JOIN "DM"."Manufacturer"  m   ON m."Id"               = di."ManufacturerId" AND m."IsDeleted" = FALSE
LEFT JOIN "DM"."Representation" r  ON r."Id"               = d."RepresentationId" AND r."IsDeleted" = FALSE
LEFT JOIN "DM"."Address"        a  ON a."Id"               = r."AddressId" AND a."IsDeleted" = FALSE
LEFT JOIN "DM"."Province"       p  ON p."Id"               = a."ProvinceId" AND p."IsDeleted" = FALSE
LEFT JOIN "DM"."City"           c  ON c."Id"               = a."CityId" AND c."IsDeleted" = FALSE
LEFT JOIN "DM"."SimCard"        sc ON sc."DeviceId"         = d."Id"   AND sc."IsDeleted" = FALSE
LEFT JOIN "DM"."DeviceItemHistory" dih ON dih."DeviceId"   = d."Id"   AND dih."IsDeleted" = FALSE
WHERE d."IsDeleted" = FALSE
  AND d."Id" IN (
      SELECT "Id" FROM "DM"."Device"
      WHERE "IsDeleted" = FALSE
      ORDER BY RANDOM()
      LIMIT 100
  )
ORDER BY d."CreationDate" DESC
LIMIT 25;
