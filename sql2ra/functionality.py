from typing import Union
import sqlparse
import radb
import radb.ast
from radb.parse import RAParser
from sqlparse.sql import IdentifierList, Identifier, Parenthesis
from sqlparse.tokens import Keyword, DML

def extract_columns(token_list):
    columns = []
    for token in token_list:
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                columns.append(str(identifier))
        elif isinstance(token, Identifier):
            columns.append(str(token))
    return columns
 
def extract_conditions(token_list):
    conditions = []
    for token in token_list:
        if token.ttype is Keyword and token.value.upper() == 'AND':
            conditions.append('AND')
        elif token.is_group and not isinstance(token, IdentifierList):
            conditions.extend(extract_conditions(token.tokens))
        elif token.is_comparison():
            conditions.append(str(token))
    return conditions

def extract_tables(token_list):
    tables = []
    for token in token_list:
        if isinstance(token, Identifier):
            tables.append(str(token))
    return tables

def parse_sql(sql, parsed):
    if not parsed:
      parsed_sql = sqlparse.parse(sql)[0]
    else:
      parsed_sql = sql
       
    columns, tables, aliases, conditions = [], [], {}, []

    # Parse tables and aliases first
    for token in parsed_sql.tokens:
        if token.ttype == sqlparse.tokens.Keyword.DML and token.value.upper() == "SELECT":
            select_clause = token.parent
            for idx, t in enumerate(select_clause.tokens):
                if isinstance(t, sqlparse.sql.IdentifierList) or isinstance(t, sqlparse.sql.Identifier):
                    if isinstance(t, sqlparse.sql.IdentifierList):
                        for identifier in t.get_identifiers():
                            if str(identifier)[0].islower() or '.' in str(identifier):
                                columns.append(str(identifier))
                    elif str(t)[0].islower() or '.' in str(t):
                        columns.append(str(t))

        if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == "FROM":
            from_clause = token.parent
            for t in from_clause.tokens:
                if isinstance(t, sqlparse.sql.Identifier) and str(t) not in columns:
                    tables.append(str(t))
                    if t.get_alias():
                        aliases[str(t)] = t.get_alias()
                elif isinstance(t, sqlparse.sql.IdentifierList):
                    for identifier in t.get_identifiers():
                        if str(identifier) not in columns:
                            tables.append(str(identifier))
                            if identifier.get_alias():
                                aliases[str(identifier)] = identifier.get_alias()

        if isinstance(token, sqlparse.sql.Where):
            for condition in token.tokens:
                if isinstance(condition, sqlparse.sql.Comparison):
                    attr = condition.left.value.strip()
                    op = RAParser.EQ
                    value = condition.right.value.strip()
                    conditions.append((attr, op, value))

    return columns, tables, aliases, conditions


def translate(statement: Union[sqlparse.sql.Statement, str], parsed = True):
    columns, tables, aliases, conditions = parse_sql(statement, parsed)
    if not tables:
      raise ValueError("No tables found in the query")

    # Create the initial relational algebra query using the first table
    ra_tables = []
    for table in tables:
        if table in aliases:
            old_name, new_name = table.split()
            ra_tables.append(radb.ast.Rename(new_name, None, radb.ast.RelRef(old_name)))
        else:
            ra_tables.append(radb.ast.RelRef(table))

    # Create the initial relational algebra query using the first table
    ra_query = ra_tables[0]

    # Add the rest of the tables using the Cross operator
    for table in ra_tables[1:]:
        ra_query = radb.ast.Cross(ra_query, table)
                
    # Apply conditions using the Select operator
    if conditions:
        combined_conditions = None
        for condition in conditions:
            attr, op, value = condition
            attr_ref = radb.ast.AttrRef(None, attr)
            val = radb.ast.RAString(value) if value.startswith("'") and value.endswith("'") else radb.ast.RANumber(value)
            new_condition = radb.ast.ValExprBinaryOp(attr_ref, op, val)
            if combined_conditions is None:
                combined_conditions = new_condition
            else:
                combined_conditions = radb.ast.ValExprBinaryOp(combined_conditions, RAParser.AND, new_condition)
        ra_query = radb.ast.Select(combined_conditions, ra_query)

    # Project the specified columns using the Project operator
    if columns and not (len(columns) == 1 and columns[0] == '*'):
        ra_columns = [radb.ast.AttrRef(None, col) for col in columns]
        ra_query = radb.ast.Project(ra_columns, ra_query)

    return ra_query