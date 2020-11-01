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


\copy transaction (order_id,date,prod_price,prod_qty,client_id,prod_id) from transaction.csv delimiter ';' csv header ;

\copy nomenclature (product_id,product_type,product_name) from nomenclature.csv delimiter ';' csv header ;
