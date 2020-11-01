\set start '2019-01-01'

drop table if exists affaire ;

CREATE TABLE affaire (
  date date,
  chiffre_affaires float
   ) ;

with tmp as (
select days.entry as date,
       transaction.prod_price * transaction.prod_qty as summed_prices
       from generate_series(date:'start',
                            date:'start' + interval '1 year' - interval '1 day',
                            interval '1 day'
       ) as days(entry)
       left join transaction on transaction.date = days.entry
)
insert into affaire  select date,sum(summed_prices) from tmp
group by date
order by date
;

\copy affaire (date,chiffre_affaires) to result.csv  delimiter ';' ;
