-- Add updated_at to CDC-tracked tables
ALTER TABLE rental     ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE payment    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE customer   ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE inventory  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

-- Auto-update updated_at on every UPDATE
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER rental_updated_at     BEFORE UPDATE ON rental     FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER payment_updated_at    BEFORE UPDATE ON payment    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER customer_updated_at   BEFORE UPDATE ON customer   FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER inventory_updated_at  BEFORE UPDATE ON inventory  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Publication for Debezium logical replication
-- publish_via_partition_root=true ensures payment partition events appear under public.payment
CREATE PUBLICATION debezium_pub FOR TABLE rental, payment, customer, inventory
    WITH (publish_via_partition_root = true);
