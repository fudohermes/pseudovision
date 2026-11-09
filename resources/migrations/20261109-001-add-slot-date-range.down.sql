ALTER TABLE schedule_slots
  DROP COLUMN active_from,
  DROP COLUMN active_until,
  DROP COLUMN is_override;
