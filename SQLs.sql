-- clean hist and reset scan id
truncate table scan cascade;
ALTER SEQUENCE scan_scan_id_seq RESTART;

