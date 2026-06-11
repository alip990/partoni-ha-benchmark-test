-- read.sql — balanced read: recent devices with status.
-- Used by MixedUser and as a baseline single-table read.
-- No parameters required.
SELECT
    d."Id"            AS device_id,
    d."DeviceStatus",
    d."Activated",
    d."CreationDate"
FROM "DM"."Device" d
WHERE d."IsDeleted" = FALSE
ORDER BY d."CreationDate" DESC
LIMIT 50;
