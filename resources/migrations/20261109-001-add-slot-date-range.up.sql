-- Date-bounded and override slots (native-engine monthly overrides).
--
-- active_from / active_until (inclusive, local dates in the scheduling zone)
-- limit the dates a slot airs on; NULL means unbounded. An is_override slot
-- replaces the base slots it overlaps on the dates it airs. Only the day-based
-- build loop (schedules whose slots are all fixed-anchor) honours these.

ALTER TABLE schedule_slots
  ADD COLUMN active_from  DATE,
  ADD COLUMN active_until DATE,
  ADD COLUMN is_override  BOOLEAN NOT NULL DEFAULT false;
