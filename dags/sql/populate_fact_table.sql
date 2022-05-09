truncate temp_fact restart identity;
insert into temp_fact
	select kode_prov,kode_kab,tanggal::date,
		unnest(array['suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina','closecontact_discarded','probable_diisolasi','probable_discarded','confirmation_sembuh','confirmation_meninggal','suspect_meninggal','closecontact_meninggal','probable_meninggal']) as "case",
		unnest(array[suspect_diisolasi, suspect_discarded, closecontact_dikarantina,closecontact_discarded,probable_diisolasi,probable_discarded,confirmation_sembuh,confirmation_meninggal,suspect_meninggal,closecontact_meninggal,probable_meninggal]) as "count"
	from warehouse_table;

truncate fact_province_daily restart identity;
insert into fact_province_daily(province_id,case_id,date,total)
	select province_id,dc.id as case_id,"date",sum(total) as total 
	from temp_fact tf inner join dim_case dc on concat(dc.status_name,'_',dc.status_detail)=tf.case
	group by province_id,case_id,"date"
	order by province_id,case_id,"date" asc;

truncate fact_province_monthly restart identity;
insert into fact_province_monthly(province_id,case_id,month,total)
	select province_id,dc.id as case_id,to_char(date,'YYYY-MM') as month,sum(total) as total 
	from temp_fact tf inner join dim_case dc on concat(dc.status_name,'_',dc.status_detail)=tf.case
	group by province_id,case_id,month
	order by province_id,case_id,month asc;

truncate fact_province_yearly restart identity;
insert into fact_province_yearly(province_id,case_id,year,total)
	select province_id,dc.id as case_id,extract(year from date) as year,sum(total) as total 
	from temp_fact tf inner join dim_case dc on concat(dc.status_name,'_',dc.status_detail)=tf.case
	group by province_id,case_id,year
	order by province_id,case_id,year asc;

truncate fact_district_monthly restart identity;
insert into fact_district_monthly(district_id,case_id,month,total)
	select district_id,dc.id as case_id,to_char(date,'YYYY-MM') as month,sum(total) as total 
	from temp_fact tf inner join dim_case dc on concat(dc.status_name,'_',dc.status_detail)=tf.case
	group by district_id,case_id,month
	order by district_id,case_id,month asc;

truncate fact_district_yearly restart identity;
insert into fact_district_yearly(district_id,case_id,year,total)
	select district_id,dc.id as case_id,extract(year from date) as year,sum(total) as total 
	from temp_fact tf inner join dim_case dc on concat(dc.status_name,'_',dc.status_detail)=tf.case
	group by district_id,case_id,year
	order by district_id,case_id,year asc;

drop table temp_fact,warehouse_table;
