drop table if exists transaction ;

CREATE TABLE transaction (
  date date,
  order_id int,
   client_id int,
   prod_id int,
   prod_price float,
   prod_qty float
   ) ;

drop table if exists nomenclature ;
create table nomenclature (
product_id int ,
product_type text ,
product_name text
) ;


\copy transaction (date,prod_price,prod_qty) from transaction.csv delimiter ';' csv header ;
