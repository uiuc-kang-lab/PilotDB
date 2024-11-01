import logging
import re

import sqlglot
from sqlglot import exp

from pilotdb.pilot_engine.commons import *


class Pilot_Rewriter:
    def __init__(self, table_cols, table_size, database):
        self.table_cols = table_cols
        self.table_size = table_size
        self.database = database

        self.subquery_count = 0
        self.page_id_rank = 0
        self.page_id_count = 0
        self.alias = {}
        self.table_alias = {}
        self.cte = {}
        self.is_rewritable = True
        self.contains_agg = {}
        self.largest_table = None
        self.single_sample = False
        self.sampled_cte = set()
        self.select_expression_count = 0
        self.aggregator_mapping = {}
        self.alias_2_page_id = {}
        self.sqlserver_page_id_mapping = {}
        self.sqlserver_page_id_mapping_count = 0
        self.sqlserver_alias2page_id = {}

        # return parameters
        self.res_2_page_id = {}
        self.result_mapping_list = []
        self.group_cols = []
        self.subquery_dict = {}
        self.limit_value = None

    def find_alias(self, expression):
        alias_list = expression.find_all(exp.Alias)
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

    def extract_page_id(self, is_union=False, is_join=False):
        if self.database == POSTGRES:
            if is_union:
                if is_join:
                    expresion = f"'page_id_{self.page_id_rank}:' || ({self.largest_table}.ctid::text::point)[0]::int as page_id_1"
                    self.page_id_rank += 1
                    self.page_id_count = 2
                else:
                    expresion = f"'page_id_{self.page_id_rank}:' || ({self.largest_table}.ctid::text::point)[0]::int as page_id_0"
                    self.page_id_rank += 1
                    self.page_id_count = 1
            else:
                expresion = f"'page_id_{self.page_id_rank}:' || ({self.largest_table}.ctid::text::point)[0]::int as page_id_{self.page_id_count}"
                self.page_id_rank += 1
                self.page_id_count += 1
            return sqlglot.parse_one(expresion)
        elif self.database == DUCKDB:
            if is_union:
                if is_join:
                    expresion = f"'page_id_{self.page_id_rank}:' || floor({self.largest_table}.rowid/2048) as page_id_1"
                    self.page_id_rank += 1
                    self.page_id_count = 2
                else:
                    expresion = f"'page_id_{self.page_id_rank}:' || floor({self.largest_table}.rowid/2048) as page_id_0"
                    self.page_id_rank += 1
                    self.page_id_count = 1
            else:
                expresion = f"'page_id_{self.page_id_rank}:' || floor({self.largest_table}.rowid/2048) as page_id_{self.page_id_count}"
                self.page_id_rank += 1
                self.page_id_count += 1
            return sqlglot.parse_one(expresion)
        elif self.database == SQLSERVER:
            if is_union:
                if is_join:
                    expresion = f"""'page_id_{self.page_id_rank}:' + CAST(CAST(SUBSTRING({self.largest_table}.%%physloc%%, 6, 1)
                    + SUBSTRING({self.largest_table}.%%physloc%%, 5, 1) AS int) AS VARCHAR) 
                    + '||' + CAST(CAST(SUBSTRING({self.largest_table}.%%physloc%%, 4, 1) 
                    + SUBSTRING({self.largest_table}.%%physloc%%, 3, 1) + SUBSTRING({self.largest_table}.%%physloc%%, 2, 1) 
                    + SUBSTRING({self.largest_table}.%%physloc%%, 1, 1) AS int) AS VARCHAR)"""
                    self.sqlserver_alias2page_id["page_id_1"] = expresion
                    expresion = f"""{expresion} AS page_id_1"""
                    self.page_id_rank += 1
                    self.page_id_count = 2
                else:
                    expresion = f"""'page_id_{self.page_id_rank}:' + CAST(CAST(SUBSTRING({self.largest_table}.%%physloc%%, 6, 1)
                    + SUBSTRING({self.largest_table}.%%physloc%%, 5, 1) AS int) AS VARCHAR) 
                    + '||' + CAST(CAST(SUBSTRING({self.largest_table}.%%physloc%%, 4, 1) 
                    + SUBSTRING({self.largest_table}.%%physloc%%, 3, 1) + SUBSTRING({self.largest_table}.%%physloc%%, 2, 1) 
                    + SUBSTRING({self.largest_table}.%%physloc%%, 1, 1) AS int) AS VARCHAR)"""
                    self.sqlserver_alias2page_id["page_id_0"] = expresion
                    expresion = f"""{expresion} AS page_id_0"""
                    self.page_id_rank += 1
                    self.page_id_count = 1
            else:
                expresion = f"""'page_id_{self.page_id_rank}:' + CAST(CAST(SUBSTRING({self.largest_table}.%%physloc%%, 6, 1)
                    + SUBSTRING({self.largest_table}.%%physloc%%, 5, 1) AS int) AS VARCHAR) 
                    + '||' + CAST(CAST(SUBSTRING({self.largest_table}.%%physloc%%, 4, 1) 
                    + SUBSTRING({self.largest_table}.%%physloc%%, 3, 1) + SUBSTRING({self.largest_table}.%%physloc%%, 2, 1) 
                    + SUBSTRING({self.largest_table}.%%physloc%%, 1, 1) AS int) AS VARCHAR)"""
                self.sqlserver_alias2page_id[f"page_id_{self.page_id_count}"] = (
                    expresion
                )
                expresion = f"""{expresion} AS page_id_{self.page_id_count}"""
                self.page_id_rank += 1
                self.page_id_count += 1
            sqlserver_page_id_mapping = (
                f"sqlserver_page_id_mapping_{self.sqlserver_page_id_mapping_count}"
            )
            self.sqlserver_page_id_mapping[sqlserver_page_id_mapping] = expresion
            self.sqlserver_page_id_mapping_count += 1
            return sqlglot.parse_one(sqlserver_page_id_mapping)

    def remove_clauses(self, expression):
        if expression.find(exp.Limit):
            self.limit_value = int(expression.args["limit"].args["expression"].this)

        expression.set("limit", None)
        expression.set("offset", None)
        expression.set("order", None)

    def rewrite_subtraction(self, expression):
        new_select_expression = []
        new_expressions = []
        for sub_expression in expression.find_all(exp.Sub):
            left = sub_expression.this.find(exp.Column)
            right = sub_expression.expression.find(exp.Column)
            if (
                left
                and right
                and left.this.this in self.alias
                and right.this.this in self.alias
            ):
                if isinstance(self.alias[right.this.this], exp.AggFunc):
                    node = sub_expression
                    while not isinstance(node, exp.Select):
                        node = node.parent
                    node_expressions = []
                    for node_select_expression in node.args["expressions"]:
                        if node_select_expression.find(exp.Sub):
                            sub_left = node_select_expression.find(exp.Sub)
                            sub_right = node_select_expression.find(exp.Sub).expression
                            sub_left.parent.parent.set("this", sub_left.this)
                            node_expressions.append(node_select_expression)
                            node_expressions.append(
                                exp.Alias(this=sub_right, alias=sub_right.this.this)
                            )
                            if (
                                exp.Sum(this=sub_right.find(exp.Column))
                                not in new_select_expression
                            ):
                                new_select_expression.append(
                                    exp.Sum(this=sub_right.find(exp.Column))
                                )
                        else:
                            node_expressions.append(node_select_expression)
                    node.set("expressions", node_expressions)

        for select_expression in new_select_expression:
            new_expressions.append(
                exp.Alias(
                    this=select_expression, alias=f"r{self.select_expression_count}"
                )
            )
            self.select_expression_count += 1

        return new_expressions

    def rewrite_select_expression(self, expression):
        number_of_avg = 0
        new_expressions = []
        ratio_type = None

        for select_expression in expression.args["expressions"]:
            is_average = False
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
                    if div_operator.this.find(
                        exp.AggFunc
                    ) and div_operator.expression.find(exp.AggFunc):
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
                    if mul_operator.this.find(
                        exp.AggFunc
                    ) and mul_operator.expression.find(exp.AggFunc):
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
                    alias_expression = exp.Alias(
                        this=temp_exp.this,
                        alias=exp.Identifier(this=f"r{self.select_expression_count}"),
                    )
                else:
                    alias_expression = exp.Alias(
                        this=temp_exp,
                        alias=exp.Identifier(this=f"r{self.select_expression_count}"),
                    )
                self.select_expression_count += 1
                new_expressions.append(alias_expression)

            result_mapping = {}
            if ratio_type == DIV_OPERATOR:
                result_mapping[AGGREGATE] = DIV_OPERATOR
                result_mapping[FIRST_ELEMENT] = f"r{self.select_expression_count-2}"
                result_mapping[SECOND_ELEMENT] = f"r{self.select_expression_count-1}"
            elif ratio_type == MUL_OPERATOR:
                result_mapping[AGGREGATE] = MUL_OPERATOR
                result_mapping[FIRST_ELEMENT] = f"r{self.select_expression_count-2}"
                result_mapping[SECOND_ELEMENT] = f"r{self.select_expression_count-1}"
            else:
                if is_average:
                    result_mapping[AGGREGATE] = AVG_OPERATOR
                    result_mapping[PAGE_SUM] = f"r{self.select_expression_count-1}"
                elif select_expression.find(exp.Sum):
                    result_mapping[AGGREGATE] = SUM_OPERATOR
                    result_mapping[PAGE_SUM] = f"r{self.select_expression_count-1}"
                elif select_expression.find(exp.Count):
                    result_mapping[AGGREGATE] = COUNT_OPERATOR
                    result_mapping[PAGE_SIZE] = f"r{self.select_expression_count-1}"
                elif select_expression.find(exp.Anonymous):
                    if (
                        select_expression.find(exp.Anonymous).this.upper()
                        == "COUNT_BIG"
                    ):
                        result_mapping[AGGREGATE] = COUNT_OPERATOR
                        result_mapping[PAGE_SIZE] = f"r{self.select_expression_count-1}"
                elif (
                    isinstance(select_expression, exp.Expression)
                    and isinstance(select_expression.this, exp.Expression)
                    and select_expression.this.this in self.aggregator_mapping
                ):
                    if self.aggregator_mapping[select_expression.this.this].find(
                        exp.Sum
                    ):
                        result_mapping[AGGREGATE] = SUM_OPERATOR
                        result_mapping[PAGE_SUM] = f"r{self.select_expression_count-1}"
                    elif self.aggregator_mapping[select_expression.this.this].find(
                        exp.Count
                    ):
                        result_mapping[AGGREGATE] = COUNT_OPERATOR
                        result_mapping[PAGE_SIZE] = f"r{self.select_expression_count-1}"

            if result_mapping:
                self.result_mapping_list.append(result_mapping)
            else:
                self.group_cols.append(f"r{self.select_expression_count-1}")

        if number_of_avg > 0:
            if self.database == SQLSERVER:
                count_expression = sqlglot.parse_one("COUNT_BIG(*)")
            else:
                count_expression = sqlglot.parse_one("COUNT(*)")
            alias_expression = exp.Alias(
                this=count_expression,
                alias=exp.Identifier(this=f"r{self.select_expression_count}"),
            )
            self.select_expression_count += 1
            new_expressions.append(alias_expression)

            for result_mapping in self.result_mapping_list:
                if result_mapping[AGGREGATE] == AVG_OPERATOR:
                    result_mapping[PAGE_SIZE] = f"r{self.select_expression_count-1}"

        # rewrite subtraction operator

        new_select_expression = self.rewrite_subtraction(expression)
        if new_select_expression:
            self.result_mapping_list[-1] = {
                AGGREGATE: SUB_OPERATOR,
                FIRST_ELEMENT: f"r{self.select_expression_count-2}",
                SECOND_ELEMENT: f"r{self.select_expression_count-1}",
            }
        new_expressions += new_select_expression
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

    def add_page_id(
        self, expression, add_group_by=True, page_id=True, is_union=False, is_join=False
    ):
        if page_id:
            page_exp = self.extract_page_id(is_union, is_join)
            for select_expression in expression.args["expressions"]:
                if select_expression.find(exp.Alias):
                    self.alias_2_page_id[select_expression.find(exp.Alias).alias] = (
                        f"page_id_{self.page_id_count-1}"
                    )
            expression.args["expressions"].append(page_exp)

            if add_group_by or "group" in expression.args:
                self.add_page_id_to_group_by(
                    expression, f"page_id_{self.page_id_count-1}"
                )

        else:
            length = self.page_id_count
            for i in range(length):
                column = f"page_id_{i}"
                page_exp = exp.Column(this=exp.Identifier(this=column))
                if (
                    expression
                    and expression.parent
                    and isinstance(expression.parent.parent, exp.Join)
                    and length == 1
                ):
                    page_exp = exp.Alias(
                        this=page_exp,
                        alias=exp.Identifier(this=f"page_id_{self.page_id_count}"),
                    )
                    self.page_id_count += 1
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
        table_list = {
            table.this.this: table for table in self.find_all_tables(expression)
        }
        self.largest_table = list(table_list.keys())[0]
        for largest_table in self.table_size:
            if largest_table in table_list:
                if table_list[largest_table].find(exp.TableAlias):
                    self.largest_table = (
                        table_list[largest_table].find(exp.TableAlias).this.this
                    )
                else:
                    self.largest_table = largest_table
                break

        for table in expression.args["from"].find_all(exp.Table):
            if table.this.this == self.largest_table:
                tablesample.set("this", table)
                expression.args["from"].set("this", tablesample)
                return expression
        if "joins" in expression.args:
            for join in expression.args["joins"]:
                for table in join.find_all(exp.Table):
                    if table.this.this == self.largest_table:
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
                            new_cte_with_alias_list = []
                            cte_to_alias = {y: x for x, y in self.cte.items()}
                            for temp_cte in new_cte_expression:
                                cte_alis = cte_to_alias[temp_cte]
                                new_cte_with_alias_list.append(
                                    exp.CTE(
                                        this=temp_cte,
                                        alias=exp.TableAlias(
                                            this=exp.Identifier(this=cte_alis)
                                        ),
                                    )
                                )
                            new_cte.set("expressions", new_cte_with_alias_list)
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

    def subquery_in_from(self, expression, is_union=False, is_join=False):
        self.subquery_in_where(expression, self.table_cols)
        self.add_table_sample(expression)
        if expression.find(exp.AggFunc) or (
            expression.find(exp.Anonymous)
            and expression.find(exp.Anonymous).this.upper() == "COUNT_BIG"
        ):
            self.add_page_id(expression, True, True, is_union, is_join)
        else:
            self.add_page_id(expression, False, True, is_union, is_join)

        return expression

    def primary_query_rewriter(
        self, expression, is_union=False, level=0, is_join=False
    ):
        contains_agg = False
        for select_expression in expression.args["expressions"]:
            if select_expression.find(exp.AggFunc) or (
                select_expression.find(exp.Anonymous)
                and select_expression.find(exp.Anonymous).this.upper() == "COUNT_BIG"
            ):
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
                        self.primary_query_rewriter(
                            join_expression.find(exp.Select), is_union, level + 1
                        )
            is_aggregate = False
            is_star = False
            for select_expression in expression.args["expressions"]:
                if select_expression.find(exp.AggFunc) or (
                    select_expression.find(exp.Anonymous)
                    and select_expression.find(exp.Anonymous).this.upper()
                    == "COUNT_BIG"
                ):
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
                self.primary_query_rewriter(cte_expression, is_union, level + 1)
                self.sampled_cte.add(expression.args["from"].this.this.this)

            if not self.single_sample:
                if "joins" in expression.args:
                    for join_expression in expression.args["joins"]:
                        if join_expression.this.this.this in self.cte:
                            cte_expression = self.cte[join_expression.this.this.this]
                            if join_expression.this.this.this not in self.sampled_cte:
                                self.primary_query_rewriter(
                                    cte_expression, is_union, level + 1, True
                                )
                                self.sampled_cte.add(join_expression.this.this.this)

            is_aggregate = False

            for select_expression in expression.args["expressions"]:
                if select_expression.find(exp.AggFunc) or (
                    select_expression.find(exp.Anonymous)
                    and select_expression.find(exp.Anonymous).this.upper()
                    == "COUNT_BIG"
                ):
                    is_aggregate = True
            if is_aggregate:
                self.add_page_id(expression, add_group_by=True, page_id=False)
            else:
                self.add_page_id(
                    expression, add_group_by=False, page_id=False, is_union=False
                )
        else:
            self.subquery_in_from(expression, is_union, is_join)
        expression.set("having", None)
        return expression

    def remove_cte(self, expression):
        remove_cte = True
        for table in expression.args["from"].find_all(exp.Table):
            if table.this.this in self.cte:
                remove_cte = False
                break

        if "join" in expression.args:
            for join_expression in expression.args["join"]:
                for table in join_expression.find_all(exp.Table):
                    if table.this.this in self.cte:
                        remove_cte = False
                        break

        if remove_cte:
            expression.set("with", None)

    def replace_sample_method(self, sql_query):
        new_query = sql_query.replace(
            "TABLESAMPLE SYSTEM (1 ROWS)", "{sampling_method}"
        )
        return new_query

    def remove_duplicate(self, expression):
        new_expressions = []
        select_expression_dict = dict()
        res_2_res_mapping = dict()
        for select_expression in expression.args["expressions"]:
            if select_expression.find(exp.Alias):
                alias = select_expression.alias
                if select_expression.this not in select_expression_dict:
                    select_expression_dict[select_expression.this] = alias
                    new_expressions.append(select_expression)
                else:
                    res_2_res_mapping[alias] = select_expression_dict[
                        select_expression.this
                    ]
            else:
                new_expressions.append(select_expression)
        expression.set("expressions", new_expressions)

        for res_mapping in self.result_mapping_list:
            for key in [FIRST_ELEMENT, SECOND_ELEMENT, PAGE_SUM, PAGE_SIZE]:
                if key in res_mapping:
                    if res_mapping[key] in res_2_res_mapping:
                        res_mapping[key] = res_2_res_mapping[res_mapping[key]]

    def fix_parse(self, sql):
        sql = sql.replace("AS days", "days")
        return sql

    def sqlserver_replace_page_id(self, old_query):
        old_query = old_query.replace("COUNT(", "COUNT_BIG(")
        for key, value in self.sqlserver_page_id_mapping.items():
            old_query = old_query.replace(key, value)
        return old_query

    def sqlserver_replace_group_by(self, expression):
        for group_by in expression.find_all(exp.Group):
            replacement = False
            for select_expression in group_by.parent.args["expressions"]:
                if select_expression.find(exp.Column):
                    column_name = select_expression.find(exp.Column).this.this
                    if column_name in self.sqlserver_page_id_mapping:
                        replacement = True
            if replacement:
                new_group_by = []
                group_arg = ""
                if "expressions" in group_by.args:
                    group_expressions = group_by.args["expressions"]
                    group_arg = "expressions"
                elif "rollup" in group_by.args:
                    group_expressions = group_by.args["rollup"]
                    group_arg = "rollup"
                for group_by_expression in group_expressions:
                    if group_by_expression.find(exp.Column):
                        column_name = group_by_expression.find(exp.Column).this.this
                        if column_name[:7] == "page_id":
                            new_group_by.append(
                                exp.Column(
                                    this=exp.Identifier(
                                        this=f"replace_{column_name[8:]}"
                                    )
                                )
                            )
                            self.sqlserver_page_id_mapping[
                                f"replace_{column_name[8:]}"
                            ] = self.sqlserver_alias2page_id[column_name]
                        else:
                            new_group_by.append(group_by_expression)
                    else:
                        new_group_by.append(group_by_expression)

                group_by.set(group_arg, new_group_by)

    def rewrite(self, original_query):
        if self.database == SQLSERVER:
            match = re.search(r"TOP (\d+)", original_query, re.IGNORECASE)
            if match:
                self.limit_value = int(match.group(1))  # The number x to be retrieved
                original_query = re.sub(
                    r"TOP \d+", "", original_query, flags=re.IGNORECASE
                )
        expression = sqlglot.parse_one(original_query)
        # Simple count query in duckdb
        if self.database == DUCKDB:
            if not expression.find(exp.Column):
                self.is_rewritable = False
                return original_query

        self.find_alias(expression)
        self.extract_cte(expression)
        self.find_all_aggregator(expression)

        self.remove_clauses(expression)

        expression = self.subquery_in_where(expression, self.table_cols)
        expression = self.rewrite_select_expression(expression)

        expression = self.primary_query_rewriter(expression)

        self.remove_cte(expression)
        self.remove_duplicate(expression)
        self.sqlserver_replace_group_by(expression)
        modified_query = expression.sql()
        new_query = self.replace_sample_method(modified_query)
        new_query = self.fix_parse(new_query)

        if self.database == SQLSERVER:
            new_query = self.sqlserver_replace_page_id(new_query)
        elif self.database == POSTGRES:
            pattern = r"\b(INTERVAL) '(\d+)' (DAYS)\b"
            new_query = re.sub(pattern, r"\1 '\2 \3'", new_query)
        return new_query

    def log_info(self):
        logging.info(f"column mapping: {self.result_mapping_list}")
        logging.info(f"group cols: {self.group_cols}")
        logging.info(f"subquery dict: {self.subquery_dict}")
        logging.info(f"res2pageid: {self.res_2_page_id}")
        logging.info(f"subqueries in WHERE and HAVING: {self.subquery_dict}")
        logging.info(f"limit value: {self.limit_value}")
