select
    count(*) as count_product
from
    order_products
where
    reordered = 1
    and add_to_cat_order < 5;
