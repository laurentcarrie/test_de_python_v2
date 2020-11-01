CREATE EXTENSION tablefunc;

drop table if exists client_interest ;

CREATE TABLE client_interest (
  client_id date,
  ventes_meubles int,
   ventes_deco int
) ;

DROP FUNCTION xxx ;
CREATE FUNCTION xxx(cid integer, pt text)
RETURNS text AS $$

DECLARE result_count integer;
BEGIN
    with tmp as (
        select * from transaction
        join nomenclature on transaction.prod_id = nomenclature.product_id
        where transaction.client_id = cid and nomenclature.product_type like pt
    )
    select count(order_id) into result_count from tmp
    group by (client_id,product_type) ;
    return result_count ;
END;
$$
    LANGUAGE plpgsql;



with tmp as (
select * from transaction
 join nomenclature on transaction.prod_id = nomenclature.product_id
)
select   count(order_id) from tmp
group by (client_id,product_type)
order by client_id
;



select distinct(client_id),
    xxx(client_id,'MEUBLE') as ventes_meuble,
    xxx(client_id,'DECO') as ventes_deco
    from transaction  ;


CREATE TABLE ct(id SERIAL, rowid TEXT, attribute TEXT, value TEXT);
INSERT INTO ct(rowid, attribute, value) VALUES('test1','att1','val1');
INSERT INTO ct(rowid, attribute, value) VALUES('test1','att2','val2');
INSERT INTO ct(rowid, attribute, value) VALUES('test1','att3','val3');
INSERT INTO ct(rowid, attribute, value) VALUES('test1','att4','val4');
INSERT INTO ct(rowid, attribute, value) VALUES('test2','att1','val5');
INSERT INTO ct(rowid, attribute, value) VALUES('test2','att2','val6');
INSERT INTO ct(rowid, attribute, value) VALUES('test2','att3','val7');
INSERT INTO ct(rowid, attribute, value) VALUES('test2','att4','val8');

select * from ct limit 10 ;

SELECT *
FROM crosstab(
  'select rowid, attribute, value
   from ct
   where attribute = ''att2'' or attribute = ''att3''
   order by 1,2')
AS ct(row_name text, category_1 text, category_2 text, category_3 text);
