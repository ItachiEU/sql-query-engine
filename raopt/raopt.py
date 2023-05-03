from typing import Dict, List, Optional, Set, Tuple

import radb
import radb.ast
from radb.parse import RAParser

def rule_break_up_selections(ra_query: radb.ast.Node) -> radb.ast.Node:
    if isinstance(ra_query, radb.ast.Select):
        # Break up conjunctions in the selection predicate
        flattened_conditions = break_up_conjunctions(ra_query.cond)

        # Create nested Select nodes for each condition in the flattened_conditions list
        node = ra_query.inputs[0]  # Get the input node of the current Select node
        for condition in reversed(flattened_conditions):
            node = radb.ast.Select(condition, node)

        # Apply the rule recursively to the input node
        node.inputs[0] = rule_break_up_selections(node.inputs[0])
        return node

    elif isinstance(ra_query, radb.ast.Project) or isinstance(ra_query, radb.ast.Rename):
        ra_query.inputs[0] = rule_break_up_selections(ra_query.inputs[0])
        return ra_query

    elif isinstance(ra_query, radb.ast.Cross):
        ra_query.inputs[0] = rule_break_up_selections(ra_query.inputs[0])
        ra_query.inputs[1] = rule_break_up_selections(ra_query.inputs[1])
        return ra_query

    elif isinstance(ra_query, radb.ast.RelRef):
        return ra_query

    else:
        raise ValueError("Unsupported relational algebra node type: " + str(type(ra_query)))


def break_up_conjunctions(condition: radb.ast.ValExprBinaryOp) -> List[radb.ast.ValExprBinaryOp]:
    if isinstance(condition, radb.ast.ValExprBinaryOp) and condition.op == RAParser.AND:
        return break_up_conjunctions(condition.inputs[0]) + break_up_conjunctions(condition.inputs[1])
    else:
        return [condition]

def merge_conditions(conditions: List[radb.ast.ValExprBinaryOp]) -> List[radb.ast.ValExprBinaryOp]:
    merged_conditions = []
    for condition in conditions:
        if isinstance(condition, radb.ast.ValExprBinaryOp) and condition.op == RAParser.AND:
            merged_conditions.extend(merge_conditions([condition.left, condition.right]))
        else:
            merged_conditions.append(condition)
    return merged_conditions


def get_condition_attributes(cond: radb.ast.ValExpr) -> Set[str]:
    if isinstance(cond, radb.ast.AttrRef):
        if cond.rel:
            return {f"{cond.rel}.{cond.name}"}
        else:
            return {cond.name}

    result = set()
    if hasattr(cond, 'inputs'):
        for input_ in cond.inputs:
            result.update(get_condition_attributes(input_))
    return result

def extract_attributes_from_cross(node: radb.ast.Node, dd: Dict[str, Dict[str, str]]) -> Set[str]:
    if isinstance(node, radb.ast.RelRef):
        return set(list(dd[node.rel].keys()) + [f"{node.rel}.{key}" for key in dd[node.rel].keys()])

    elif isinstance(node, radb.ast.Cross):
        left_attrs = extract_attributes_from_cross(node.inputs[0], dd)
        right_attrs = extract_attributes_from_cross(node.inputs[1], dd)
        return left_attrs.union(right_attrs)

    elif isinstance(node, radb.ast.Rename):
        original_relname = node.inputs[0].rel
        if node.relname:
            original_attrs = dd[original_relname]
            if node.attrnames:
               return set(list(node.attrnames) + [f"{node.relname}.{attr}" for attr in node.attrnames])
            else:
               return set(list(original_attrs.keys()) + [f"{node.relname}.{attr}" for attr in original_attrs.keys()])
    
    elif isinstance(node, radb.ast.Select):
        return extract_attributes_from_cross(node.inputs[0], dd)

    else:
        raise ValueError(f"Unexpected node type encountered while extracting attributes: {type(node)}")

def flatten_nested_selects(node: radb.ast.Node) -> Tuple[radb.ast.Node, List[radb.ast.Select]]:
    nested_selects = []
    while isinstance(node, radb.ast.Select):
        nested_selects.append(node)
        node = node.inputs[0]
    return node, nested_selects

def try_push_down_selection(node: radb.ast.Node, dd: Dict[str, Dict[str, str]]) -> radb.ast.Node:
    if isinstance(node, radb.ast.Select):
        child_node = try_push_down_selection(node.inputs[0], dd)

        if isinstance(child_node, radb.ast.Cross):
            r1, r2 = child_node.inputs
            attributes = get_condition_attributes(node.cond)

            r1_attrs = extract_attributes_from_cross(r1, dd)
            r2_attrs = extract_attributes_from_cross(r2, dd)
            
            pushed_down_r1 = None
            pushed_down_r2 = None

            if set(attributes).issubset(r1_attrs) and not set(attributes).intersection(r2_attrs):
                pushed_down_r1 = rule_push_down_selections(radb.ast.Select(node.cond, r1), dd)

            if set(attributes).issubset(r2_attrs) and not set(attributes).intersection(r1_attrs):
                pushed_down_r2 = rule_push_down_selections(radb.ast.Select(node.cond, r2), dd)

            if pushed_down_r1 or pushed_down_r2:
                return radb.ast.Cross(pushed_down_r1 or r1, pushed_down_r2 or r2)

        return radb.ast.Select(node.cond, child_node)

    elif isinstance(node, radb.ast.Project):
        return radb.ast.Project(node.attrs, try_push_down_selection(node.inputs[0], dd))

    elif isinstance(node, radb.ast.Cross):
        return radb.ast.Cross(try_push_down_selection(node.inputs[0], dd), try_push_down_selection(node.inputs[1], dd))

    elif isinstance(node, radb.ast.RelRef):
        return node
   
    elif isinstance(node, radb.ast.Rename):
        updated_dd = dd.copy()
        original_relname = node.inputs[0].rel
        if node.relname:
            updated_dd[node.relname] = updated_dd[original_relname]
            del updated_dd[original_relname]

        if node.attrnames:
            renamed_attrs = dict(zip(node.inputs[0].type.attrs, node.attrnames))
            updated_dd[node.relname] = {renamed_attrs[k]: v for k, v in updated_dd[node.relname].items()}

        return radb.ast.Rename(node.relname, node.attrnames, try_push_down_selection(node.inputs[0], updated_dd))


    else:
        raise ValueError("Unexpected node type encountered.")


def rule_push_down_selections(node: radb.ast.Node, dd: Dict[str, Dict[str, str]]) -> radb.ast.Node:
    project_attrs = None
    if isinstance(node, radb.ast.Project):
        project_attrs = node.attrs
        node = node.inputs[0]
    node, flattened = flatten_nested_selects(node)
    
    failed_push_selects: radb.ast.Select = []
    # Iterate through the reversed flattened list and try to push down each Select
    for select_node in reversed(flattened):
        pushed_down = try_push_down_selection(radb.ast.Select(select_node.cond, node), dd)
        if isinstance(pushed_down, radb.ast.Select):
            # Couldn't push down this Select, put it back in the flattened list
            failed_push_selects.append(pushed_down)
        else:
            # Successfully pushed down, update the result
            node = pushed_down

    result = node
    for select_node in failed_push_selects:
        result = radb.ast.Select(select_node.cond, result)

    if project_attrs:
        result = radb.ast.Project(project_attrs, result)
    return result


def rule_merge_selections(node: radb.ast.Node) -> radb.ast.Node:
    if isinstance(node, radb.ast.Select):
        child_node = rule_merge_selections(node.inputs[0])

        if isinstance(child_node, radb.ast.Select):
            # Merge the current selection with the nested selection using a conjunctive predicate
            merged_cond = radb.ast.ValExprBinaryOp(node.cond, RAParser.AND, child_node.cond)
            return radb.ast.Select(merged_cond, child_node.inputs[0])
        else:
            return radb.ast.Select(node.cond, child_node)

    elif isinstance(node, radb.ast.Project):
        return radb.ast.Project(node.attrs, rule_merge_selections(node.inputs[0]))

    elif isinstance(node, radb.ast.Cross):
        return radb.ast.Cross(rule_merge_selections(node.inputs[0]), rule_merge_selections(node.inputs[1]))

    elif isinstance(node, radb.ast.RelRef):
        return node

    elif isinstance(node, radb.ast.Rename):
        return radb.ast.Rename(node.relname, node.attrnames, rule_merge_selections(node.inputs[0]))

    else:
        raise ValueError("Unexpected node type encountered.")

def split_conditions(condition: radb.ast.ValExpr) -> List[radb.ast.ValExpr]:
    if isinstance(condition, radb.ast.ValExprBinaryOp) and condition.op == RAParser.AND:
        return split_conditions(condition.inputs[0]) + split_conditions(condition.inputs[1])
    else:
        return [condition]
    
def is_join_condition(condition: radb.ast.ValExpr) -> bool:
    if isinstance(condition, radb.ast.ValExprBinaryOp):
        return isinstance(condition.inputs[0], radb.ast.AttrRef) and isinstance(condition.inputs[1], radb.ast.AttrRef)
    return False
    
def collect_table_names(node, tables):
    if isinstance(node, radb.ast.RelRef):
        tables.add(node.rel)
    elif isinstance(node, radb.ast.Rename):
        tables.add(node.relname)
        collect_table_names(node.inputs[0], tables)
    elif isinstance(node, (radb.ast.Project, radb.ast.Select, radb.ast.Join, radb.ast.Cross)):
        for input in node.inputs:
            collect_table_names(input, tables)

def involves_both_tables(node, condition):
    left_tables = set()
    right_tables = set()

    def collect_attr_names(expr, tables):
        if isinstance(expr, radb.ast.AttrRef):
            tables.add(expr.rel)
        else:
            for input in expr.inputs:
                collect_attr_names(input, tables)

    collect_attr_names(condition.inputs[0], left_tables)
    collect_attr_names(condition.inputs[1], right_tables)

    left_node_tables = set()
    right_node_tables = set()

    collect_table_names(node.inputs[0], left_node_tables)
    collect_table_names(node.inputs[1], right_node_tables)

    left_tables = left_tables.intersection(left_node_tables)
    right_tables = right_tables.intersection(right_node_tables)

    return bool(left_tables) and bool(right_tables)

def process_cross_nodes(node: radb.ast.Node, conditions: List[radb.ast.ValExpr]) -> radb.ast.Node:
    if isinstance(node, radb.ast.Cross):
        left = process_cross_nodes(node.inputs[0], conditions)
        right = process_cross_nodes(node.inputs[1], conditions)

        join_conditions = [c for c in conditions if involves_both_tables(node, c)]

        if join_conditions:
            combined_join_condition = join_conditions[0]
            for c in join_conditions[1:]:
                combined_join_condition = radb.ast.ValExprBinaryOp(combined_join_condition, RAParser.AND, c)

            return radb.ast.Join(left, combined_join_condition, right)
        else:
            return radb.ast.Cross(left, right)
    else:
        return rule_introduce_joins(node)

def rule_introduce_joins(node: radb.ast.Node) -> radb.ast.Node:
    if isinstance(node, radb.ast.Select):
        child_node = rule_introduce_joins(node.inputs[0])
        conditions = split_conditions(node.cond)

        new_child_node = process_cross_nodes(child_node, conditions)
        non_join_conditions = [c for c in conditions if not is_join_condition(c)]

        if non_join_conditions:
            cond = non_join_conditions[0]
            for c in non_join_conditions[1:]:
                cond = radb.ast.ValExprBinaryOp(cond, RAParser.AND, c)
            return radb.ast.Select(cond, new_child_node)
        else:
            return new_child_node

    elif isinstance(node, radb.ast.Project):
        return radb.ast.Project(node.attrs, rule_introduce_joins(node.inputs[0]))

    elif isinstance(node, radb.ast.Cross):
        return radb.ast.Cross(rule_introduce_joins(node.inputs[0]), rule_introduce_joins(node.inputs[1]))

    elif isinstance(node, radb.ast.RelRef):
        return node

    elif isinstance(node, radb.ast.Rename):
        return radb.ast.Rename(node.relname, node.attrnames, rule_introduce_joins(node.inputs[0]))
    
    elif isinstance(node, radb.ast.Join):
        return node

    else:
        raise ValueError("Unexpected node type encountered.")