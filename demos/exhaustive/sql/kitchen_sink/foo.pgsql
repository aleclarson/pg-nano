CREATE TYPE status_type AS ENUM (
  'pending',
  'active',
  'inactive',
  'archived'
);

CREATE TYPE address_type AS (
  street varchar (100),
  city varchar (50),
  state varchar (50),
  zip_code varchar (20)
);

CREATE TABLE foo (
  id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  name varchar(100) NOT NULL,
  description text,
  created_at timestamp with time zone NOT NULL DEFAULT current_timestamp,
  updated_at timestamp with time zone NOT NULL DEFAULT current_timestamp,
  is_active boolean NOT NULL DEFAULT TRUE,
  score numeric(5, 2),
  tags text[],
  matrix doubleprecision[][],
  metadata jsonb,
  color_preference varchar(20) CHECK (
    color_preference IN ('red', 'green', 'blue')
  ),
  binary_data bytea,
  coordinates point,
  ip_address inet,
  mac_address macaddr,
  price_range int4range,
  schedule tstzrange,
  priority smallint CHECK (priority BETWEEN 1 AND 5),
  uuid uuid DEFAULT gen_random_uuid(),
  search_vector tsvector,
  status status_type DEFAULT 'pending',
  address address_type,
  product_attributes hstore
);
