import sqlglot
from sqlglot import exp
from pilotdb.pilot_engine.commons import *
from pilotdb.pilot_engine.query_base import Query

class query_sampling:
    def __init__(self, table_cols, table_size, database):
        self.table_cols = table_cols
        self.table_size = table_size
        self.database = database
        
        self.subquery_count = 0
        self.alias = {}
        self.table_alias = {}
        self.cte = {}
        self.largest_table = None
        self.single_sample = False
        self.sampled_cte = set()
        self.aggregator_mapping = {}
        
        self.subquery_dict = {}


    def find_alias(self, expression):
        alias_list = expression.find_all(exp.Alias,bfs=False)
        for alias in alias_list:
            self.alias[alias.alias] = alias.this
        for table in expression.find_all(exp.Table):
            if table.alias:
                self.table_alias[table.alias] = table
        for alia, value in self.alias.items():
            if isinstance(value, exp.Column) and value.this.this in self.alias:
                self.alias[alia] = self.alias[value.this.this]

        return None


    def extract_cte(self, expression):
        cte_list = expression.find_all(exp.CTE)
        for cte in cte_list:
            self.cte[cte.alias] = cte.this


    def find_all_aggregator(self, expression):
        for agg in expression.find_all(exp.AggFunc):
            self.aggregator_mapping[agg.parent.alias] = agg
            table_set = set()
            for column in agg.find_all(exp.Column):
                table_set.add(column.table)
            if len(table_set) > 1:
                self.single_sample = True
        

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
                return True
        if "joins" in expression.args:
            for join in expression.args["joins"]:
                for table in join.find_all(exp.Table):
                    if table.this.this == largest_table:
                        table_parent = table.parent
                        tablesample.set("this", table)
                        table_parent.set("this", tablesample)
                        return True
        return False


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
                subquery_2_name = {y: x for x, y in self.subquery_dict.items()}
                for table in tables_in_from:
                    if table.this.this in column_information:
                        column_list += column_information[table.this.this]
                    else:
                        if table.this.this in self.cte:
                            new_cte_expression = []
                            new_cte_expression.append(self.cte[table.this.this])
                            for table_in_cte in self.cte[table.this.this].find_all(
                                exp.Table
                            ):
                                if table_in_cte.this.this in self.cte:
                                    new_cte_expression.insert(
                                        0, self.cte[table_in_cte.this.this]
                                    )
                            new_cte = exp.With()
                            new_cte.set("expressions", new_cte_expression)
                            new_subquery = subquery.copy()
                            new_subquery.set("with", new_cte)
                            if new_subquery.sql() in subquery_2_name:
                                subquery_exp = sqlglot.parse_one(
                                    subquery_2_name[new_subquery.sql()]
                                )
                                subquery.parent.replace(subquery_exp)
                            else:
                                subquery_str = f"subquery_{self.subquery_count}"
                                self.subquery_count += 1
                                self.subquery_dict[subquery_str] = new_subquery.sql()
                                subquery_exp = sqlglot.parse_one(subquery_str)
                                subquery.parent.replace(subquery_exp)

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

    def add_sample_rate(self, expression):
        new_select_expression_list = []
        for select_expression in expression.args["expressions"]:
            if select_expression.find(exp.Div):
              div_operator = select_expression.find(exp.Div)
              if div_operator.this.find(exp.AggFunc) and div_operator.expression.find(exp.AggFunc):
                new_select_expression_list.append(select_expression)
                continue
            if select_expression.find(exp.Sum) or select_expression.find(exp.Count):
                agg_expression = select_expression.find(exp.AggFunc)
                col = agg_expression.find(exp.Column)
                if col and col.this.this in self.alias:
                    original_agg_expression = self.alias[col.this.this]
                    if original_agg_expression.find(exp.Sum) or original_agg_expression.find(exp.Count):
                        new_select_expression_list.append(select_expression)
                        continue
                agg_expression_parent = agg_expression.parent
                new_div_expression = exp.Div(
                    this=agg_expression,
                    expression=sqlglot.parse_one("sample_rate"),
                )
                if isinstance(agg_expression_parent, exp.Select):
                  new_select_expression_list.append(new_div_expression)
                else:
                  agg_expression_parent.set("this", new_div_expression)
                  new_select_expression_list.append(select_expression)
            else:
                new_select_expression_list.append(select_expression)
        expression.set("expressions", new_select_expression_list)
                
                
    def subquery_in_from(self, expression, is_union=False, is_join=False):
        self.subquery_in_where(expression, self.table_cols)
        if self.add_table_sample(expression):
          self.add_sample_rate(expression)

        return expression


    def primary_query_rewriter(self, expression, is_union=False, level=0, is_join=False):

        if expression.find(exp.Union):
            is_union = True
        if expression.args["from"].find(exp.Subquery):
            if expression.args["from"].find(exp.Union):
                for select_query in expression.args["from"].find_all(
                    exp.Select, bfs=False
                ):
                    is_in_where = False
                    node = select_query
                    while node:
                        if isinstance(node, exp.Where):
                            is_in_where = True
                            break
                        node = node.parent
                    if not is_in_where:
                        self.primary_query_rewriter(select_query, is_union, level + 1)
            else:
                select_query = expression.args["from"].find(exp.Select)
                self.primary_query_rewriter(select_query, is_union, level + 1)
            if "joins" in expression.args:
                for join_expression in expression.args["joins"]:
                    if join_expression.find(exp.Select):
                        self.primary_query_rewriter(join_expression.find(exp.Select), is_union, level + 1)
            self.add_sample_rate(expression)

        elif self.cte and expression.args["from"].this.this.this in self.cte:
            cte_expression = self.cte[expression.args["from"].this.this.this]
            if expression.args["from"].this.this.this not in self.sampled_cte:
                self.primary_query_rewriter(cte_expression, is_union, level + 1)
                self.sampled_cte.add(expression.args["from"].this.this.this)

            if not self.single_sample:
                if "joins" in expression.args:
                    for join_expression in expression.args["joins"]:
                        if join_expression.this.this.this in self.cte:
                            cte_expression = self.cte[join_expression.this.this.this]
                            if join_expression.this.this.this not in self.sampled_cte:
                                self.primary_query_rewriter(cte_expression, is_union, level + 1, True)
                                self.sampled_cte.add(join_expression.this.this.this)  

            self.add_sample_rate(expression)
        else:
            self.subquery_in_from(expression, is_union, is_join)

        return expression
                   

    def remove_cte(self, expression):
        if expression.find(exp.With):
          cte_alias_list = []
          new_cte_expression_list = set()
          for table in expression.args["from"].find_all(exp.Table):
              if table.this.this in self.cte:
                  cte_alias_list.append(table.this.this)
                  new_cte_expression_list.add(self.cte[table.this.this].parent)

          if "joins" in expression.args:
              for join_expression in expression.args["joins"]:
                  for table in join_expression.find_all(exp.Table):
                      if table.this.this in self.cte:
                          cte_alias_list.append(table.this.this)
                          new_cte_expression_list.add(self.cte[table.this.this].parent)

          for cte_expression in new_cte_expression_list:
              for cte_table in cte_expression.find_all(exp.Table):
                  if cte_table.this.this in self.cte:
                      cte_alias_list.append(cte_table.this.this)
          new_ctes = []
          for old_cte in expression.args["with"].expressions:
              if old_cte.alias in cte_alias_list:
                  new_ctes.append(old_cte)
          if new_ctes:
            new_with_expression = exp.With(expressions=new_ctes)
            expression.set("with", new_with_expression)
          else:
            expression.set("with", None)


    def replace_sample_method(self, sql_query):
        new_query = sql_query.replace("TABLESAMPLE SYSTEM (1 ROWS)", "sampling_method")
        return new_query


    def modify_having(self, expression):
        for having_expression in expression.find_all(exp.Having):
            if having_expression.find(exp.Sum) or having_expression.find(exp.Count):
                agg_expression = having_expression.find(exp.AggFunc)
                col = agg_expression.find(exp.Column)
                if col and col.this.this in self.alias:
                    original_agg_expression = self.alias[col.this.this]
                    if original_agg_expression.find(exp.Sum) or original_agg_expression.find(exp.Count):
                        continue
                agg_expression_parent = agg_expression.parent
                new_div_expression = exp.Div(
                    this=agg_expression,
                    expression=sqlglot.parse_one("sample_rate"),
                )
                agg_expression_parent.set("this", new_div_expression)

 
    def rewrite(self, original_query):
        expression = sqlglot.parse_one(original_query)
        self.find_alias(expression)
        self.extract_cte(expression)
        self.find_all_aggregator(expression)

        expression = self.primary_query_rewriter(expression)

        self.remove_cte(expression)
        self.modify_having(expression)
        modified_query = expression.sql()
        new_query = self.replace_sample_method(modified_query)
        return new_query


if __name__ == "__main__":
    import json
    tpch_list = [1,4,5,6,7,8,9,11,12,14,17,19,22]
    tpcds_list = [2, 3, 6, 10, 13, 17, 19, 25,  29, 32, 33, 42, 43, 45, 48, 50, 52, 55, 61, 62, 66, 67, 76, 77, 80, 85, 86, 88, 90, 91, 92, 96, 97, 99, '24a', '24b', '14a', '14b', '23a', '23b']
    for i in tpcds_list:
      query_file = f"../../benchmarks/tpcds/query_{i}.sql"
      meta_file = "../../benchmarks/tpcds/meta.json"

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

      qr = query_sampling(meta["table_cols"], meta["table_size"], POSTGRES)

      modified_query = qr.rewrite(sql)
      # with open(f'test_{i}.sql', 'w') as f:
      #     f.write(modified_query)
 
      with open(f'/Users/jun/Desktop/research/PilotDB/answer/tpcds/test_{i}.sql', 'r') as f:
          template = f.read()

      if modified_query != template:
          print(i)

