SELECT DISTINCT(i_product_name)
FROM item i1
WHERE i_manufact_id BETWEEN 748 AND 748 + 40
  AND (
    SELECT COUNT(*) AS item_cnt
    FROM item
    WHERE (
        i_manufact = i1.i_manufact
        AND (
          (
            i_category = 'Women'
            AND (
              i_color = 'gainsboro'
              OR i_color = 'aquamarine'
            )
            AND (
              i_units = 'Ounce'
              OR i_units = 'Dozen'
            )
            AND (
              i_size = 'medium'
              OR i_size = 'economy'
            )
          )
          OR (
            i_category = 'Women'
            AND (
              i_color = 'chiffon'
              OR i_color = 'violet'
            )
            AND (
              i_units = 'Ton'
              OR i_units = 'Pound'
            )
            AND (
              i_size = 'extra large'
              OR i_size = 'small'
            )
          )
          OR (
            i_category = 'Men'
            AND (
              i_color = 'chartreuse'
              OR i_color = 'blue'
            )
            AND (
              i_units = 'Each'
              OR i_units = 'Oz'
            )
            AND (
              i_size = 'N/A'
              OR i_size = 'large'
            )
          )
          OR (
            i_category = 'Men'
            AND (
              i_color = 'tan'
              OR i_color = 'dodger'
            )
            AND (
              i_units = 'Bunch'
              OR i_units = 'Tsp'
            )
            AND (
              i_size = 'medium'
              OR i_size = 'economy'
            )
          )
        )
      )
      OR (
        i_manufact = i1.i_manufact
        AND (
          (
            i_category = 'Women'
            AND (
              i_color = 'blanched'
              OR i_color = 'tomato'
            )
            AND (
              i_units = 'Tbl'
              OR i_units = 'Case'
            )
            AND (
              i_size = 'medium'
              OR i_size = 'economy'
            )
          )
          OR (
            i_category = 'Women'
            AND (
              i_color = 'almond'
              OR i_color = 'lime'
            )
            AND (
              i_units = 'Box'
              OR i_units = 'Dram'
            )
            AND (
              i_size = 'extra large'
              OR i_size = 'small'
            )
          )
          OR (
            i_category = 'Men'
            AND (
              i_color = 'peru'
              OR i_color = 'saddle'
            )
            AND (
              i_units = 'Pallet'
              OR i_units = 'Gram'
            )
            AND (
              i_size = 'N/A'
              OR i_size = 'large'
            )
          )
          OR (
            i_category = 'Men'
            AND (
              i_color = 'indian'
              OR i_color = 'spring'
            )
            AND (
              i_units = 'Unknown'
              OR i_units = 'Carton'
            )
            AND (
              i_size = 'medium'
              OR i_size = 'economy'
            )
          )
        )
      )
  ) > 0
ORDER BY i_product_name
LIMIT 100;