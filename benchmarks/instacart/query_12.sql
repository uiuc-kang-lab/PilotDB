select
    order_dow,
    sum(case
        when reordered = 1
            then 1
        else 0
    end) as reordered_count,
    sum(case
        when reordered = 0
            then 1
        else 0
    end) as non_reordered_count
from
    orders,
    order_products
where
    orders.order_id = order_products.order_id
    and add_to_cat_order <= 2
    and order_hour_of_day >= 12
    and order_hour_of_day < 18
group by
    order_dow
order by
    order_dow;
