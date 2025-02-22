CREATE COLLATION numeric (PROVIDER = ICU, LOCALE = 'en-u-kn-true');

CREATE TABLE t (
  a text,
  b text COLLATE numeric,
  c text COLLATE numeric
);
