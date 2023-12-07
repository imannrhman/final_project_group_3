DROP TABLE IF EXISTS dimensional.dim_date;
CREATE TABLE dimensional.dim_date (
  date_key INTEGER PRIMARY KEY,
  date DATE NOT NULL,
  day_of_week INTEGER NOT NULL,
  week INTEGER NOT NULL,
  month INTEGER NOT NULL,
  quarter INTEGER NOT NULL,
  year INTEGER NOT NULL
);
