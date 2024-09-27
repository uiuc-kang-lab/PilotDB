select
    reordered,
    count(*) as count_order
from
    order_products
where
    add_to_cat_order <= 5
group by
    reordered
order by
    reordered;
