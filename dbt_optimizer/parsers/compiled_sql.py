from dataclasses import dataclass, field
from typing import List, Dict
import sqlglot
from run_result import RunResults


@dataclass
class CTE:
    name: str
    query: str
    dependencies: List[str] = field(
        default_factory=list
    )  # Tables/CTEs used in this CTE


@dataclass
class SQLQuery:
    ctes: List[CTE] = field(default_factory=list)
    main_query: str = ""
    dependencies: Dict[str, List[str]] = field(
        default_factory=dict
    )  # CTE dependency graph

    def add_cte(self, name: str, query: str, dependencies: List[str]):
        self.ctes.append(CTE(name=name, query=query, dependencies=dependencies))

    def set_main_query(self, query: str):
        self.main_query = query


class SQLParser:

    @staticmethod
    def parse_sql_recursively(sql: str):
        sql = sql.replace("`", "")
        # Check if there is a WITH clause
        if "with" not in sql.lower():
            return sqlglot.parse_one(sql)  # Base case, just parse the final query

        # Handle CTE parsing recursively
        start_idx = sql.lower().find("with")  # Find the first "WITH"
        with_clause = sql[start_idx:]  # Get everything starting from the WITH clause

        # Look for the closing parenthesis for the outermost CTE
        open_paren = with_clause.find("(")
        close_paren = with_clause.find(")", open_paren)

        # Recursively process inner WITH clauses
        nested_sql = with_clause[open_paren + 1 : close_paren]  # Extract the inner SQL
        inner_parsed = SQLParser.parse_sql_recursively(
            nested_sql
        )  # Recursively parse inner query

        # Replace the nested query with the parsed version
        with_clause = (
            with_clause[: open_paren + 1]
            + str(inner_parsed)
            + with_clause[close_paren:]
        )

        # Now parse the rest of the outer SQL (the part after the closing parenthesis)
        remaining_sql = sql[start_idx + len(with_clause) :]

        # Recurse over the remaining SQL
        remaining_parsed = (
            SQLParser.parse_sql_recursively(remaining_sql) if remaining_sql else ""
        )

        # Return the final parsed SQL combining both parts
        return sqlglot.parse_one(with_clause + remaining_parsed)

    @staticmethod
    def parse_sql(sql: str) -> SQLQuery:
        """
        Parses a SQL string into a SQLQuery dataclass.
        """

        # Parse the SQL using sqlglot
        parsed = sqlglot.parse_one(sql)

        # Initialize SQLQuery object
        sql_query = SQLQuery()

        # Extract CTEs
        ctes = parsed.find_all("with")
        for cte in ctes:
            cte_name = cte.alias_or_name
            cte_query = str(cte)
            cte_dependencies = [ref.alias_or_name for ref in cte.find_all("table")]
            sql_query.add_cte(
                name=cte_name, query=cte_query, dependencies=cte_dependencies
            )

        # Extract main query (everything after the CTEs)
        main_query = parsed.find("select")
        if main_query:
            sql_query.set_main_query(str(main_query))

        # Build dependency graph
        for cte in sql_query.ctes:
            sql_query.dependencies[cte.name] = cte.dependencies

        return sql_query


print(RunResults.get_compiled_sql())
print("-----------------------------------------------")
print(SQLParser.parse_sql_recursively(sql=RunResults.get_compiled_sql()))
