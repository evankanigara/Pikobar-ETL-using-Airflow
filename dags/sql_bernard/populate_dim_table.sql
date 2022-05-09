truncate dim_province;
insert into dim_province 
	select distinct kode_prov, nama_prov from warehouse_table;

truncate dim_district;
insert into dim_district
	select distinct kode_kab,kode_prov,nama_kab from warehouse_table
	order by kode_kab asc;
