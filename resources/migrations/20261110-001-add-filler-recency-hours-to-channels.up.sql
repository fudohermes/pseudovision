-- Per-channel filler recency window override, in hours.
-- NULL = use the global default (4 hours). A short window (e.g. 1) makes the
-- bin-packer treat items as "recent" for less time, so channels with a small
-- filler supply repeat items sooner instead of exhausting their pool.
ALTER TABLE channels ADD COLUMN filler_recency_hours integer;
