insert into broker (broker_id, descr, webpage, status, api_key, class_name) values (9,'Wise', 'https://wise.com/','1','','ScrapeWise');
insert into broker (broker_id, descr, webpage, status, api_key, class_name) values (9,'WorldRemit', ' ','1','','ScrapeWorldRemit');

drop table rate cascade;
alter table hist drop rate_id cascade;
alter table hist RENAME COLUMN buy_rate TO buy_qty; 
alter table hist RENAME COLUMN sell_rate TO sell_qty; 