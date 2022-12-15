insert into broker (broker_id, descr, webpage, api_key, class_name) values (8,'Revolut', 'https://www.bankmillennium.pl/','','');

commit;


alter table broker add column class_name character varying(255)