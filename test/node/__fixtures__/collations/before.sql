CREATE COLLATION posix FROM "POSIX";

CREATE TABLE t (
  a text,
  b text COLLATE posix
);

CREATE COLLATION numeric (PROVIDER = ICU, LOCALE = 'en-u-kn-true');
