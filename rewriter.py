import sqlglot
from sqlglot import exp

AVG_OPERATOR = "avg"
SUM_OPERATOR = "sum"
COUNT_OPERATOR = "count"
DIV_OPERATOR = "div"
MUL_OPERATOR = "mul"
AGGREGATE = "aggregate"
PAGE_SUM = "page_sum"
PAGE_SIZE = "page_size"
FIRST_ELEMENT = "first_element"
SECOND_ELEMENT = "second_element"


class query_rewrite:
    def __init__(self, table_cols, table_size):
        self.table_cols = table_cols
        self.table_size = table_size
        self.subquery_count = 0
        self.page_id_rank = 0
        self.page_id_count = 0
        self.subquery_dict = {}
        self.alias = {}
        self.table_alias = {}
        self.cte = {}
        self.is_rewritable = True
        self.contains_agg = {}
        self.largest_table = None
        self.single_sample = False
        self.sampled_cte = set()
        self.select_expression_count = 0
        self.result_mapping_list = []
        self.group_cols = []
        self.aggregator_mapping = {}
        self.alias_2_page_id = {}
        self.res_2_page_id = {}
        
        
    def find_alias(self, expression):
        alias_list = expression.find_all(exp.Alias)
        for alias in alias_list:
            self.alias[alias.alias] = alias.this
        for table in expression.find_all(exp.Table):
            if table.alias:
                self.table_alias[table.alias] = table

        return None


    def extract_cte(self, expression):
        cte_list = expression.find_all(exp.CTE)
        for cte in cte_list:
            self.cte[cte.alias] = cte.this


    def find_all_aggregator(self, expression):
        for agg in expression.find_all(exp.AggFunc):
            print(57, repr(agg.parent))
            self.aggregator_mapping[agg.parent.alias] = agg
            table_set = set()
            for column in agg.find_all(exp.Column):
                table_set.add(column.table)
            if len(table_set) > 1:
              self.single_sample = True
              
              
    def extract_page_id(self, table, database, is_union=False):
        if database == "postgres":
            if is_union:
                expresion = f"'page_id_{self.page_id_rank}:' || ({table}.ctid::text::point)[0]::int as page_id_0"
                self.page_id_rank += 1
                self.page_id_count = 1
            else:
                expresion = f"'page_id_{self.page_id_rank}:' || ({table}.ctid::text::point)[0]::int as page_id_{self.page_id_count}"
                self.page_id_rank += 1
                self.page_id_count += 1
            return sqlglot.parse_one(expresion)

    def remove_order(self, expression):
        expression.set("order", None)
        return expression

    def remove_limit(self, expression):
        expression.set("limit", None)
        return expression

    
    def replace_star(self, expression):
        new_expressions = []
        print(91, repr(expression))
        for select_expression in expression.args["expressions"]:
            if isinstance(select_expression, exp.Star):
              for subquery in expression.find_all(exp.Subquery):
                for sub_select_expression in subquery.this.args["expressions"]:
                  if sub_select_expression.find(exp.Alias):
                    new_expressions.append(exp.Column(this=exp.Identifier(this=sub_select_expression.alias)))
              expression.set("expressions", new_expressions)
        print(98, expression)


    def replace_avg(self, expression):
        number_of_avg = 0
        new_expressions = []
        is_average = False
        ratio_type = None

        for select_expression in expression.args["expressions"]:            
            temp_expressions = []
            
            if select_expression.find(exp.Avg):
                avg_expression = select_expression.find(exp.Avg)
                sum_expression = exp.Sum(this=avg_expression.this)
                temp_expressions.append(sum_expression)
                number_of_avg += 1
                is_average = True
            else:
              div_operator = select_expression.find(exp.Div)
              mul_operator = select_expression.find(exp.Mul)
              if div_operator:
                  if div_operator.this.find(exp.AggFunc) and div_operator.expression.find(
                      exp.AggFunc
                  ):
                      ratio_type = DIV_OPERATOR
                      temp_expressions.append(div_operator.this)
                      temp_expressions.append(div_operator.expression)
                  else:
                      left = div_operator.this.find(exp.Column)
                      right = div_operator.expression.find(exp.Column)
                      if (
                          left
                          and right
                          and left.this.this in self.alias
                          and isinstance(self.alias[left.this.this], exp.AggFunc)
                          and right.this.this in self.alias
                          and isinstance(self.alias[right.this.this], exp.AggFunc)
                      ):
                          ratio_type = DIV_OPERATOR
                          temp_expressions.append(div_operator.this)
                          temp_expressions.append(div_operator.expression)
                      else:
                          temp_expressions.append(select_expression)
              elif mul_operator:
                  if mul_operator.this.find(exp.AggFunc) and mul_operator.expression.find(
                      exp.AggFunc
                  ):
                      ratio_type = MUL_OPERATOR
                      temp_expressions.append(mul_operator.this)
                      temp_expressions.append(mul_operator.expression)
                  else:
                      left = mul_operator.this.find(exp.Column)
                      right = mul_operator.expression.find(exp.Column)
                      if (
                          left
                          and right
                          and left.this.this in self.alias
                          and isinstance(self.alias[left.this.this], exp.AggFunc)
                          and right.this.this in self.alias
                          and isinstance(self.alias[right.this.this], exp.AggFunc)
                      ):
                          ratio_type = MUL_OPERATOR
                          temp_expressions.append(mul_operator.this)
                          temp_expressions.append(mul_operator.expression)
                      else:
                          temp_expressions.append(select_expression)
              else:
                  temp_expressions.append(select_expression)
            
            for temp_exp in temp_expressions:
                if isinstance(temp_exp, exp.Alias):
                  alias_expression = exp.Alias(this=temp_exp.this, alias=exp.Identifier(this=f'r{self.select_expression_count}'))
                else:
                  alias_expression = exp.Alias(this=temp_exp, alias=exp.Identifier(this=f'r{self.select_expression_count}'))
                self.select_expression_count += 1
                new_expressions.append(alias_expression)
            
            result_mapping = {}
            if ratio_type == DIV_OPERATOR:
                result_mapping[AGGREGATE] = DIV_OPERATOR
                result_mapping[FIRST_ELEMENT] = f'r{self.select_expression_count-2}'
                result_mapping[SECOND_ELEMENT] = f'r{self.select_expression_count-1}'
            elif ratio_type == MUL_OPERATOR:
                result_mapping[AGGREGATE] = MUL_OPERATOR
                result_mapping[FIRST_ELEMENT] = f'r{self.select_expression_count-2}'
                result_mapping[SECOND_ELEMENT] = f'r{self.select_expression_count-1}'
            else:
              if is_average:
                  result_mapping[AGGREGATE] = AVG_OPERATOR
                  result_mapping[PAGE_SUM] = f'r{self.select_expression_count-1}'
              elif select_expression.find(exp.Sum):
                  result_mapping[AGGREGATE] = SUM_OPERATOR
                  result_mapping[PAGE_SUM] = f'r{self.select_expression_count-1}'
              elif select_expression.find(exp.Count):
                  result_mapping[AGGREGATE] = COUNT_OPERATOR
                  result_mapping[PAGE_SIZE] = f'r{self.select_expression_count-1}'
              elif select_expression.this.this in self.aggregator_mapping:
                if self.aggregator_mapping[select_expression.this.this].find(exp.Sum):
                  result_mapping[AGGREGATE] = SUM_OPERATOR
                  result_mapping[PAGE_SUM] = f'r{self.select_expression_count-1}'
                elif self.aggregator_mapping[select_expression.this.this].find(exp.Count):
                  result_mapping[AGGREGATE] = COUNT_OPERATOR
                  result_mapping[PAGE_SIZE] = f'r{self.select_expression_count-1}'
            

            if result_mapping:
              self.result_mapping_list.append(result_mapping)
            else:
              self.group_cols.append(f'r{self.select_expression_count-1}')
        
        if number_of_avg > 0:
            count_expression = sqlglot.parse_one("COUNT(*)")
            alias_expression = exp.Alias(this=count_expression, alias=exp.Identifier(this=f'r{self.select_expression_count}'))
            self.select_expression_count += 1
            new_expressions.append(alias_expression)
            
            for result_mapping in self.result_mapping_list:
                if result_mapping[AGGREGATE] == AVG_OPERATOR:
                    result_mapping[PAGE_SIZE] = f'r{self.select_expression_count-1}'
        expression.set("expressions", new_expressions)
        
        return expression

    
    def add_page_id_to_group_by(self, expression, page_id_name):
      page_col = exp.Column(this=exp.Identifier(this=page_id_name))
      if "group" in expression.args:
          if "rollup" in expression.args["group"].args:
              expression.args["group"].args["rollup"].append(page_col)
          else:
              expression.args["group"].args["expressions"].append(page_col)
      else:
          group_by_expr = exp.Group(expressions=[page_col])
          expression.set("group", group_by_expr)
      
    
    def add_page_id(self, expression, add_group_by=True, page_id=True, is_union=False):
        if page_id:
            page_exp = self.extract_page_id(self.largest_table, "postgres", is_union)
            for select_expression in expression.args["expressions"]:
                if select_expression.find(exp.Alias):
                    self.alias_2_page_id[select_expression.find(exp.Alias).alias] = f"page_id_{self.page_id_count-1}"
            expression.args["expressions"].append(page_exp)

            if add_group_by:
                self.add_page_id_to_group_by(expression, f"page_id_{self.page_id_count-1}")

        else:
            for i in range(self.page_id_count):
                column = f"page_id_{i}"
                page_exp = exp.Column(this=exp.Identifier(this=column))
                expression.args["expressions"].append(page_exp)
                if add_group_by:
                  self.add_page_id_to_group_by(expression, f"page_id_{i}")

        return expression


    def find_all_tables(self, expression):
        table_list = []
        for table in expression.args["from"].find_all(exp.Table):
            table_list.append(table)
        if "joins" in expression.args:
            for join in expression.args["joins"]:
                table_list.append(join.find(exp.Table))
        return table_list

    def add_table_sample(self, expression):
        tablesample = (
            sqlglot.parse_one("from lineitem TABLESAMPLE SYSTEM (1)").args["from"].this
        )
        table_list = [table.this.this for table in self.find_all_tables(expression)]

        for largest_table in self.table_size:
            if largest_table in table_list:
                self.largest_table = largest_table
                break

        for table in expression.args["from"].find_all(exp.Table):
            if table.this.this == largest_table:
                tablesample.set("this", table)
                expression.args["from"].set("this", tablesample)
                return expression
        if "joins" in expression.args:
            for join in expression.args["joins"]:
                for table in join.find_all(exp.Table):
                    if table.this.this == largest_table:
                        table_parent = table.parent
                        tablesample.set("this", table)
                        table_parent.set("this", tablesample)
                        return expression

    def extract_items(self, expression, type):
        extracted_items = []
        for item in expression.find_all(type):
            extracted_items.append(item)
        return extracted_items

    def subquery_in_where(self, expression, column_information):
        if "where" in expression.args:
            subquery_list = expression.args["where"].find_all(exp.Select)
            for subquery in subquery_list:
                tables_in_from = self.find_all_tables(subquery)

                column_list = []
                for table in tables_in_from:
                    column_list += column_information[table.this.this]

                tables_in_subquery = self.extract_items(subquery, exp.Table)
                columns_in_subquery = self.extract_items(subquery, exp.Column)

                is_separable = True
                for table in tables_in_subquery:
                    if table not in tables_in_from:
                        is_separable = False
                for column in columns_in_subquery:
                    if column.this.this not in column_list:
                        is_separable = False
                    if column.table:
                        if self.table_alias[column.table] not in tables_in_from:
                            is_separable = False
                if is_separable:
                    subquery_str = f"subquery_{self.subquery_count}"
                    self.subquery_count += 1
                    self.subquery_dict[subquery_str] = subquery.sql()
                    subquery_exp = sqlglot.parse_one(subquery_str)
                    subquery.parent.replace(subquery_exp)
        return expression

    def subquery_in_from(self, expression, is_union=False):
        print(220, expression)
        self.subquery_in_where(expression, self.table_cols)
        self.add_table_sample(expression)
        if expression.find(exp.AggFunc):
            self.add_page_id(expression, True, True, is_union)
        else:
            self.add_page_id(expression, False, True, is_union)

        return expression

    def page(self, expression, is_union=False, level=0):
        contains_agg = False
        for select_expression in expression.args["expressions"]:
            if select_expression.find(exp.AggFunc):
                contains_agg = True
                break
        if contains_agg:
            for i in range(level):
              if self.contains_agg[i]:
                self.is_rewritable = False
            self.contains_agg[level] = True
        elif level not in self.contains_agg:
            self.contains_agg[level] = False

        if expression.find(exp.Union):
            is_union = True

        if expression.args["from"].find(exp.Subquery):
            if expression.args["from"].find(exp.Union):
              for select_query in expression.args["from"].find_all(exp.Select, bfs=False):
                self.page(select_query, is_union, level + 1)
            else:
              select_query = expression.args["from"].find(exp.Select)
              self.page(select_query, is_union, level + 1)
            # expression = self.page(expression.args['from'].find(exp.Subquery))
            if "joins" in expression.args:
              for join_expression in expression.args["joins"]:
                if join_expression.find(exp.Select):
                    self.page(join_expression.find(exp.Select), is_union, level + 1)
            is_aggregate = False
            is_star = False
            for select_expression in expression.args["expressions"]:
                if select_expression.find(exp.AggFunc):
                    is_aggregate = True
                if select_expression.find(exp.Star):
                    is_star = True
            if is_aggregate:
                self.add_page_id(expression, add_group_by=True, page_id=False)
            elif not is_star:
                self.add_page_id(expression, add_group_by=False, page_id=False)
        elif self.cte and expression.args["from"].this.this.this in self.cte:
            cte_expression = self.cte[expression.args["from"].this.this.this]
            if expression.args["from"].this.this.this not in self.sampled_cte:   
              self.page(cte_expression, is_union, level+1)
              self.sampled_cte.add(expression.args["from"].this.this.this)
              
            if not self.single_sample:
              if "joins" in expression.args:
                  for join_expression in expression.args["joins"]:
                      if join_expression.this.this.this in self.cte:
                          cte_expression = self.cte[join_expression.this.this.this]
                          if join_expression.this.this.this not in self.sampled_cte:
                            self.page(cte_expression, is_union, level+1)
                            self.sampled_cte.add(join_expression.this.this.this)

            is_aggregate = False
            is_star = False
            for select_expression in expression.args["expressions"]:
                if select_expression.find(exp.AggFunc):
                    is_aggregate = True
                if select_expression.find(exp.Star):
                    is_star = True
            if is_aggregate:
                self.add_page_id(expression, add_group_by=True, page_id=False)
            elif not is_star:
                self.add_page_id(expression, add_group_by=False, page_id=False)

        else:
            self.subquery_in_from(expression, is_union)

        return expression

    def parse_window(self, expression):
        window = expression.find(exp.Window)
        if window:
            return True
        stddev = expression.find(exp.StddevSamp)
        if stddev:
            return True
        return False
      
      
    def extract_res_2_page_id(self, expression):
      if self.page_id_count > 1:
        for select_expression in expression.args["expressions"]:
            if select_expression.find(exp.Column):
                column = select_expression.find(exp.Column)
                if column.this.this in self.alias_2_page_id:
                    self.res_2_page_id[select_expression.alias] = self.alias_2_page_id[column.this.this]
        
      
      
    def rewrite(self, original_query):
        expression = sqlglot.parse_one(original_query)
        if self.parse_window(expression):
            return original_query
        self.find_alias(expression)
        self.extract_cte(expression)
        self.find_all_aggregator(expression)
        self.replace_star(expression)
        
        
        expression = self.remove_order(expression)
        expression = self.remove_limit(expression)
        expression = self.subquery_in_where(expression, table_cols)
        expression = self.replace_avg(expression)

        expression = self.page(expression)
        self.extract_res_2_page_id(expression)
        print(266, expression)
        modified_query = expression.sql()

        return modified_query


table_cols = {
    "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    "region": ["r_regionkey", "r_name", "r_comment"],
    "supplier": [
        "s_suppkey",
        "s_name",
        "s_address",
        "s_nationkey",
        "s_phone",
        "s_acctbal",
        "s_comment",
    ],
    "customer": [
        "c_custkey",
        "c_name",
        "c_address",
        "c_nationkey",
        "c_phone",
        "c_acctbal",
        "c_mktsegment",
        "c_comment",
    ],
    "part": [
        "p_partkey",
        "p_name",
        "p_mfgr",
        "p_brand",
        "p_type",
        "p_size",
        "p_container",
        "p_retailprice",
        "p_comment",
    ],
    "partsupp": [
        "ps_partkey",
        "ps_suppkey",
        "ps_availqty",
        "ps_supplycost",
        "ps_comment",
    ],
    "orders": [
        "o_orderkey",
        "o_custkey",
        "o_orderstatus",
        "o_totalprice",
        "o_orderdate",
        "o_orderpriority",
        "o_clerk",
        "o_shippriority",
        "o_comment",
    ],
    "lineitem": [
        "l_orderkey",
        "l_partkey",
        "l_suppkey",
        "l_linenumber",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
        "l_shipinstruct",
        "l_shipmode",
        "l_comment",
    ],
}

table_size = [
    "lineitem",
    "orders",
    "partsupp",
    "part",
    "customer",
    "supplier",
    "nation",
    "region",
]


# table_cols = {
#     "dbgen_version": [
#         "dv_version",
#         "dv_create_date",
#         "dv_create_time",
#         "dv_cmdline_args",
#     ],
#     "customer_address": [
#         "ca_address_sk",
#         "ca_address_id",
#         "ca_street_number",
#         "ca_street_name",
#         "ca_street_type",
#         "ca_suite_number",
#         "ca_city",
#         "ca_county",
#         "ca_state",
#         "ca_zip",
#         "ca_country",
#         "ca_gmt_offset",
#         "ca_location_type",
#     ],
#     "customer_demographics": [
#         "cd_demo_sk",
#         "cd_gender",
#         "cd_marital_status",
#         "cd_education_status",
#         "cd_purchase_estimate",
#         "cd_credit_rating",
#         "cd_dep_count",
#         "cd_dep_employed_count",
#         "cd_dep_college_count",
#     ],
#     "date_dim": [
#         "d_date_sk",
#         "d_date_id",
#         "d_date",
#         "d_month_seq",
#         "d_week_seq",
#         "d_quarter_seq",
#         "d_year",
#         "d_dow",
#         "d_moy",
#         "d_dom",
#         "d_qoy",
#         "d_fy_year",
#         "d_fy_quarter_seq",
#         "d_fy_week_seq",
#         "d_day_name",
#         "d_quarter_name",
#         "d_holiday",
#         "d_weekend",
#         "d_following_holiday",
#         "d_first_dom",
#         "d_last_dom",
#         "d_same_day_ly",
#         "d_same_day_lq",
#         "d_current_day",
#         "d_current_week",
#         "d_current_month",
#         "d_current_quarter",
#         "d_current_year",
#     ],
#     "warehouse": [
#         "w_warehouse_sk",
#         "w_warehouse_id",
#         "w_warehouse_name",
#         "w_warehouse_sq_ft",
#         "w_street_number",
#         "w_street_name",
#         "w_street_type",
#         "w_suite_number",
#         "w_city",
#         "w_county",
#         "w_state",
#         "w_zip",
#         "w_country",
#         "w_gmt_offset",
#     ],
#     "ship_mode": [
#         "sm_ship_mode_sk",
#         "sm_ship_mode_id",
#         "sm_type",
#         "sm_code",
#         "sm_carrier",
#         "sm_contract",
#     ],
#     "time_dim": [
#         "t_time_sk",
#         "t_time_id",
#         "t_time",
#         "t_hour",
#         "t_minute",
#         "t_second",
#         "t_am_pm",
#         "t_shift",
#         "t_sub_shift",
#         "t_meal_time",
#     ],
#     "reason": ["r_reason_sk", "r_reason_id", "r_reason_desc"],
#     "income_band": ["ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"],
#     "item": [
#         "i_item_sk",
#         "i_item_id",
#         "i_rec_start_date",
#         "i_rec_end_date",
#         "i_item_desc",
#         "i_current_price",
#         "i_wholesale_cost",
#         "i_brand_id",
#         "i_brand",
#         "i_class_id",
#         "i_class",
#         "i_category_id",
#         "i_category",
#         "i_manufact_id",
#         "i_manufact",
#         "i_size",
#         "i_formulation",
#         "i_color",
#         "i_units",
#         "i_container",
#         "i_manager_id",
#         "i_product_name",
#     ],
#     "store": [
#         "s_store_sk",
#         "s_store_id",
#         "s_rec_start_date",
#         "s_rec_end_date",
#         "s_closed_date_sk",
#         "s_store_name",
#         "s_number_employees",
#         "s_floor_space",
#         "s_hours",
#         "s_manager",
#         "s_market_id",
#         "s_geography_class",
#         "s_market_desc",
#         "s_market_manager",
#         "s_division_id",
#         "s_division_name",
#         "s_company_id",
#         "s_company_name",
#         "s_street_number",
#         "s_street_name",
#         "s_street_type",
#         "s_suite_number",
#         "s_city",
#         "s_county",
#         "s_state",
#         "s_zip",
#         "s_country",
#         "s_gmt_offset",
#         "s_tax_precentage",
#     ],
#     "call_center": [
#         "cc_call_center_sk",
#         "cc_call_center_id",
#         "cc_rec_start_date",
#         "cc_rec_end_date",
#         "cc_closed_date_sk",
#         "cc_open_date_sk",
#         "cc_name",
#         "cc_class",
#         "cc_employees",
#         "cc_sq_ft",
#         "cc_hours",
#         "cc_manager",
#         "cc_mkt_id",
#         "cc_mkt_class",
#         "cc_mkt_desc",
#         "cc_market_manager",
#         "cc_division",
#         "cc_division_name",
#         "cc_company",
#         "cc_company_name",
#         "cc_street_number",
#         "cc_street_name",
#         "cc_street_type",
#         "cc_suite_number",
#         "cc_city",
#         "cc_county",
#         "cc_state",
#         "cc_zip",
#         "cc_country",
#         "cc_gmt_offset",
#         "cc_tax_percentage",
#     ],
#     "customer": [
#         "c_customer_sk",
#         "c_customer_id",
#         "c_current_cdemo_sk",
#         "c_current_hdemo_sk",
#         "c_current_addr_sk",
#         "c_first_shipto_date_sk",
#         "c_first_sales_date_sk",
#         "c_salutation",
#         "c_first_name",
#         "c_last_name",
#         "c_preferred_cust_flag",
#         "c_birth_day",
#         "c_birth_month",
#         "c_birth_year",
#         "c_birth_country",
#         "c_login",
#         "c_email_address",
#         "c_last_review_date_sk",
#     ],
#     "web_site": [
#         "web_site_sk",
#         "web_site_id",
#         "web_rec_start_date",
#         "web_rec_end_date",
#         "web_name",
#         "web_open_date_sk",
#         "web_close_date_sk",
#         "web_class",
#         "web_manager",
#         "web_mkt_id",
#         "web_mkt_class",
#         "web_mkt_desc",
#         "web_market_manager",
#         "web_company_id",
#         "web_company_name",
#         "web_street_number",
#         "web_street_name",
#         "web_street_type",
#         "web_suite_number",
#         "web_city",
#         "web_county",
#         "web_state",
#         "web_zip",
#         "web_country",
#         "web_gmt_offset",
#         "web_tax_percentage",
#     ],
#     "store_returns": [
#         "sr_returned_date_sk",
#         "sr_return_time_sk",
#         "sr_item_sk",
#         "sr_customer_sk",
#         "sr_cdemo_sk",
#         "sr_hdemo_sk",
#         "sr_addr_sk",
#         "sr_store_sk",
#         "sr_reason_sk",
#         "sr_ticket_number",
#         "sr_return_quantity",
#         "sr_return_amt",
#         "sr_return_tax",
#         "sr_return_amt_inc_tax",
#         "sr_fee",
#         "sr_return_ship_cost",
#         "sr_refunded_cash",
#         "sr_reversed_charge",
#         "sr_store_credit",
#         "sr_net_loss",
#     ],
#     "household_demographics": [
#         "hd_demo_sk",
#         "hd_income_band_sk",
#         "hd_buy_potential",
#         "hd_dep_count",
#         "hd_vehicle_count",
#     ],
#     "web_page": [
#         "wp_web_page_sk",
#         "wp_web_page_id",
#         "wp_rec_start_date",
#         "wp_rec_end_date",
#         "wp_creation_date_sk",
#         "wp_access_date_sk",
#         "wp_autogen_flag",
#         "wp_customer_sk",
#         "wp_url",
#         "wp_type",
#         "wp_char_count",
#         "wp_link_count",
#         "wp_image_count",
#         "wp_max_ad_count",
#     ],
#     "promotion": [
#         "p_promo_sk",
#         "p_promo_id",
#         "p_start_date_sk",
#         "p_end_date_sk",
#         "p_item_sk",
#         "p_cost",
#         "p_response_target",
#         "p_promo_name",
#         "p_channel_dmail",
#         "p_channel_email",
#         "p_channel_catalog",
#         "p_channel_tv",
#         "p_channel_radio",
#         "p_channel_press",
#         "p_channel_event",
#         "p_channel_demo",
#         "p_channel_details",
#         "p_purpose",
#         "p_discount_active",
#     ],
#     "catalog_page": [
#         "cp_catalog_page_sk",
#         "cp_catalog_page_id",
#         "cp_start_date_sk",
#         "cp_end_date_sk",
#         "cp_department",
#         "cp_catalog_number",
#         "cp_catalog_page_number",
#         "cp_description",
#         "cp_type",
#     ],
#     "inventory": [
#         "inv_date_sk",
#         "inv_item_sk",
#         "inv_warehouse_sk",
#         "inv_quantity_on_hand",
#     ],
#     "catalog_returns": [
#         "cr_returned_date_sk",
#         "cr_returned_time_sk",
#         "cr_item_sk",
#         "cr_refunded_customer_sk",
#         "cr_refunded_cdemo_sk",
#         "cr_refunded_hdemo_sk",
#         "cr_refunded_addr_sk",
#         "cr_returning_customer_sk",
#         "cr_returning_cdemo_sk",
#         "cr_returning_hdemo_sk",
#         "cr_returning_addr_sk",
#         "cr_call_center_sk",
#         "cr_catalog_page_sk",
#         "cr_ship_mode_sk",
#         "cr_warehouse_sk",
#         "cr_reason_sk",
#         "cr_order_number",
#         "cr_return_quantity",
#         "cr_return_amount",
#         "cr_return_tax",
#         "cr_return_amt_inc_tax",
#         "cr_fee",
#         "cr_return_ship_cost",
#         "cr_refunded_cash",
#         "cr_reversed_charge",
#         "cr_store_credit",
#         "cr_net_loss",
#     ],
#     "web_returns": [
#         "wr_returned_date_sk",
#         "wr_returned_time_sk",
#         "wr_item_sk",
#         "wr_refunded_customer_sk",
#         "wr_refunded_cdemo_sk",
#         "wr_refunded_hdemo_sk",
#         "wr_refunded_addr_sk",
#         "wr_returning_customer_sk",
#         "wr_returning_cdemo_sk",
#         "wr_returning_hdemo_sk",
#         "wr_returning_addr_sk",
#         "wr_web_page_sk",
#         "wr_reason_sk",
#         "wr_order_number",
#         "wr_return_quantity",
#         "wr_return_amt",
#         "wr_return_tax",
#         "wr_return_amt_inc_tax",
#         "wr_fee",
#         "wr_return_ship_cost",
#         "wr_refunded_cash",
#         "wr_reversed_charge",
#         "wr_account_credit",
#         "wr_net_loss",
#     ],
#     "web_sales": [
#         "ws_sold_date_sk",
#         "ws_sold_time_sk",
#         "ws_ship_date_sk",
#         "ws_item_sk",
#         "ws_bill_customer_sk",
#         "ws_bill_cdemo_sk",
#         "ws_bill_hdemo_sk",
#         "ws_bill_addr_sk",
#         "ws_ship_customer_sk",
#         "ws_ship_cdemo_sk",
#         "ws_ship_hdemo_sk",
#         "ws_ship_addr_sk",
#         "ws_web_page_sk",
#         "ws_web_site_sk",
#         "ws_ship_mode_sk",
#         "ws_warehouse_sk",
#         "ws_promo_sk",
#         "ws_order_number",
#         "ws_quantity",
#         "ws_wholesale_cost",
#         "ws_list_price",
#         "ws_sales_price",
#         "ws_ext_discount_amt",
#         "ws_ext_sales_price",
#         "ws_ext_wholesale_cost",
#         "ws_ext_list_price",
#         "ws_ext_tax",
#         "ws_coupon_amt",
#         "ws_ext_ship_cost",
#         "ws_net_paid",
#         "ws_net_paid_inc_tax",
#         "ws_net_paid_inc_ship",
#         "ws_net_paid_inc_ship_tax",
#         "ws_net_profit",
#     ],
#     "catalog_sales": [
#         "cs_sold_date_sk",
#         "cs_sold_time_sk",
#         "cs_ship_date_sk",
#         "cs_bill_customer_sk",
#         "cs_bill_cdemo_sk",
#         "cs_bill_hdemo_sk",
#         "cs_bill_addr_sk",
#         "cs_ship_customer_sk",
#         "cs_ship_cdemo_sk",
#         "cs_ship_hdemo_sk",
#         "cs_ship_addr_sk",
#         "cs_call_center_sk",
#         "cs_catalog_page_sk",
#         "cs_ship_mode_sk",
#         "cs_warehouse_sk",
#         "cs_item_sk",
#         "cs_promo_sk",
#         "cs_order_number",
#         "cs_quantity",
#         "cs_wholesale_cost",
#         "cs_list_price",
#         "cs_sales_price",
#         "cs_ext_discount_amt",
#         "cs_ext_sales_price",
#         "cs_ext_wholesale_cost",
#         "cs_ext_list_price",
#         "cs_ext_tax",
#         "cs_coupon_amt",
#         "cs_ext_ship_cost",
#         "cs_net_paid",
#         "cs_net_paid_inc_tax",
#         "cs_net_paid_inc_ship",
#         "cs_net_paid_inc_ship_tax",
#         "cs_net_profit",
#     ],
#     "store_sales": [
#         "ss_sold_date_sk",
#         "ss_sold_time_sk",
#         "ss_item_sk",
#         "ss_customer_sk",
#         "ss_cdemo_sk",
#         "ss_hdemo_sk",
#         "ss_addr_sk",
#         "ss_store_sk",
#         "ss_promo_sk",
#         "ss_ticket_number",
#         "ss_quantity",
#         "ss_wholesale_cost",
#         "ss_list_price",
#         "ss_sales_price",
#         "ss_ext_discount_amt",
#         "ss_ext_sales_price",
#         "ss_ext_wholesale_cost",
#         "ss_ext_list_price",
#         "ss_ext_tax",
#         "ss_coupon_amt",
#         "ss_net_paid",
#         "ss_net_paid_inc_tax",
#         "ss_net_profit",
#     ],
# }

# table_size = [
#     "store_sales",
#     "catalog_sales",
#     "inventory",
#     "web_sales",
#     "store_returns",
#     "store_returns",
#     "web_returns",
#     "customer_demographics",
#     "customer",
#     "customer_address",
#     "time_dim",
#     "date_dim",
# ]


query_file = "/Users/jun/Desktop/research/AQP/taired/AQP/tpc_ds_queries/query_2.sql"
# for i in [22]:#[1,4,5,6,7,8,9,12,14,17,19,22]:
[2, 3, 6, 9, 10, 13, 17, 19, 25,  29, 32, 33, 42, 43, 45, 48, 50, 52, 55, 61, 62, 66, 67, 76, 77, 80, 85, 86, 88, 90, 91, 92, 96, 97, 99, '24a', '24b']
wtih_clause = [33, 66, 77, 80], '14a', '14b'
for i in [1]:  # [1,4,5,6,7,8,9,12,14,17,19,22]:
    query_file = (
        f"/Users/jun/Desktop/research/AQP/taired/AQP/tpc_h/template/query{i}.sql"
    )
    # query_file = (
    #     f"/Users/jun/Desktop/research/AQP/taired/AQP/tpc_ds_queries/query_{i}.sql"
    # )
    with open(query_file, "r") as f:
        sql = f.read()
    qr = query_rewrite(table_cols, table_size)
    modified_query = qr.rewrite(sql)
    with open(f"./tpcds/query{i}.sql", "w") as f:
        f.write(modified_query)
    print(216, modified_query)
    print(qr.result_mapping_list)
    print(qr.subquery_dict)
    print(qr.res_2_page_id)
