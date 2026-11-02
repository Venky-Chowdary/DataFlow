-- Postgres seed for the studio schema browser.
-- sales.customers is 12 rows. public stays empty so the tree is not one schema by accident.
CREATE SCHEMA IF NOT EXISTS sales;

CREATE TABLE IF NOT EXISTS sales.customers (
  id integer PRIMARY KEY,
  email text NOT NULL,
  created_at timestamp NOT NULL
);

INSERT INTO sales.customers (id, email, created_at) VALUES
  (1, 'a1@example.com', '2026-01-01 00:00:00'),
  (2, 'a2@example.com', '2026-01-02 00:00:00'),
  (3, 'a3@example.com', '2026-01-03 00:00:00'),
  (4, 'a4@example.com', '2026-01-04 00:00:00'),
  (5, 'a5@example.com', '2026-01-05 00:00:00'),
  (6, 'a6@example.com', '2026-01-06 00:00:00'),
  (7, 'a7@example.com', '2026-01-07 00:00:00'),
  (8, 'a8@example.com', '2026-01-08 00:00:00'),
  (9, 'a9@example.com', '2026-01-09 00:00:00'),
  (10, 'a10@example.com', '2026-01-10 00:00:00'),
  (11, 'a11@example.com', '2026-01-11 00:00:00'),
  (12, 'a12@example.com', '2026-01-12 00:00:00')
ON CONFLICT (id) DO NOTHING;
