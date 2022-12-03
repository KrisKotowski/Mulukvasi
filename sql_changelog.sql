alter table broker add api_key character varying(255);

insert into broker (broker_id, descr, webpage, api_key) values (3,'TraderMade', 'https://tradermade.com/','DlZmo2SbGyCjzcXyaaPw');

commit;