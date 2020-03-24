alter table call_center     add constraint cc_d2    foreign key (cc_open_date_sk)           references date_dim (d_date_sk);
alter table call_center     add constraint cc_d1    foreign key (cc_closed_date_sk)         references date_dim (d_date_sk);

alter table catalog_page    add constraint cp_d2    foreign key (cp_start_date_sk)          references date_dim (d_date_sk);
alter table catalog_page    add constraint cp_d1    foreign key (cp_end_date_sk)            references date_dim (d_date_sk);

alter table customer add constraint c_cd            foreign key (c_current_cdemo_sk)        references customer_demographics (cd_demo_sk);
alter table customer add constraint c_hd            foreign key (c_current_hdemo_sk)        references household_demographics (hd_demo_sk);
alter table customer add constraint c_a             foreign key (c_current_addr_sk)         references customer_address (ca_address_sk);
alter table customer add constraint c_fsd2          foreign key (c_first_shipto_date_sk)    references date_dim (d_date_sk);
alter table customer add constraint c_fsd           foreign key (c_first_sales_date_sk)     references date_dim (d_date_sk);
alter table customer add constraint c_lrd           foreign key (c_last_review_date_sk)     references date_dim (d_date_sk);

alter table household_demographics add constraint hd_ib foreign key (hd_income_band_sk)     references income_band (ib_income_band_sk) ;

alter table promotion add constraint p_start_date   foreign key (p_start_date_sk)           references date_dim (d_date_sk);
alter table promotion add constraint p_end_date     foreign key (p_end_date_sk)             references date_dim (d_date_sk);
alter table promotion add constraint p_i            foreign key (p_item_sk)                 references item (i_item_sk);

alter table store add constraint s_close_date       foreign key (s_closed_date_sk)          references date_dim (d_date_sk);

alter table web_page add constraint wp_cd           foreign key (wp_creation_date_sk)       references date_dim (d_date_sk);
alter table web_page add constraint wp_ad           foreign key (wp_access_date_sk)         references date_dim (d_date_sk);
alter table web_page add constraint wp_c            foreign key (wp_customer_sk)            references customer (c_customer_sk);

alter table web_site add constraint web_d2          foreign key (web_open_date_sk)          references date_dim (d_date_sk);
alter table web_site add constraint web_d1          foreign key (web_close_date_sk)         references date_dim (d_date_sk);
