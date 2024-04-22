import sqlglot
from sqlglot import exp
from pilotdb.pilot_engine.commons import *


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
        for select_expression in expression.args["expressions"]:
            if isinstance(select_expression, exp.Star):
              for subquery in expression.find_all(exp.Subquery):
                for sub_select_expression in subquery.this.args["expressions"]:
                  if sub_select_expression.find(exp.Alias):
                    new_expressions.append(exp.Column(this=exp.Identifier(this=sub_select_expression.alias)))
              expression.set("expressions", new_expressions)


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
        expression = self.subquery_in_where(expression, self.table_cols)
        expression = self.replace_avg(expression)

        expression = self.page(expression)
        self.extract_res_2_page_id(expression)
        print(266, expression)
        modified_query = expression.sql()

        return modified_query



if __name__ == "__main__":
    import json
    query_file = "../../benchmarks/tpch/query_1.sql"
    meta_file = "../../benchmarks/tpch/meta.json"

    desired_modified_query = """
    select
        l_returnflag as c0,
        l_linestatus as c1,
        {page_id} as c2,
        sum(l_quantity) as c3,
        sum(l_extendedprice) as c4,
        sum(l_extendedprice * (1 - l_discount)) as c5,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as c6,
        sum(l_discount) as c7,
        count(*) as c8
    from
        lineitem {sample}
    where
        l_shipdate <= date '1998-12-01' - interval '90 day'
    group by
        l_returnflag,
        l_linestatus,
        {page_id}
    order by
        l_returnflag,
        l_linestatus;
    """

    with open(query_file, "r") as f:
        sql = f.read()

    with open(meta_file, "r") as f:
        meta = json.load(f)
    
    qr = query_rewrite(meta["table_cols"], meta["table_size"])

    modified_query = qr.rewrite(sql)
    print(modified_query)
