-- write.sql — balanced write: single device insert.
-- Used by MixedUser. No Python parameters needed — RANDOM() handles variance.
INSERT INTO "DM"."Device" ("DeviceStatus", "Activated", "CreationDate", "IsDeleted")
VALUES (
    (FLOOR(RANDOM() * 5) + 1)::integer,
    TRUE,
    NOW(),
    FALSE
)
RETURNING "Id";
