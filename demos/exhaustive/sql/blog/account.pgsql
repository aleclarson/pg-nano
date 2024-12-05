CREATE TABLE account (
  id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  username varchar(50) UNIQUE NOT NULL,
  email varchar(100) UNIQUE NOT NULL,
  password_hash varchar(255) NOT NULL,
  posts_count integer DEFAULT 0 NOT NULL,
  first_name varchar(50),
  last_name varchar(50),
  date_of_birth date,
  created_at timestamp with time zone DEFAULT current_timestamp NOT NULL,
  updated_at timestamp with time zone DEFAULT current_timestamp NOT NULL,
  last_login timestamp with time zone,
  is_deleted boolean DEFAULT FALSE
);

-- Index for faster lookups on username and email
CREATE INDEX idx_account_username ON account (username);

CREATE INDEX idx_account_email ON account (email);

-- Function to update the updated_at timestamp
CREATE FUNCTION update_account_timestamp()
RETURNS trigger
AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$
LANGUAGE plpgsql;

-- Trigger to call the update_account_timestamp function
CREATE TRIGGER update_account_timestamp
BEFORE UPDATE ON account
FOR EACH ROW
EXECUTE FUNCTION update_account_timestamp();
