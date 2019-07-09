CREATE TYPE fr.destination_resource_type AS ENUM ('bank_card', 'crypto_wallet');

ALTER TABLE fr.destination ADD COLUMN resource_type fr.destination_resource_type;
ALTER TABLE fr.destination ADD COLUMN crypto_wallet_id CHARACTER VARYING;
ALTER TABLE fr.destination ADD COLUMN crypto_wallet_currency CHARACTER VARYING;