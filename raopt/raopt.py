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

            if set(attributes).issubset(r1_attrs) and not set(attributes).intersection(r2_attrs):
                return radb.ast.Cross(try_push_down_selection(radb.ast.Select(node.cond, r1), dd), r2)

            if set(attributes).issubset(r2_attrs) and not set(attributes).intersection(r1_attrs):
                return radb.ast.Cross(r1, try_push_down_selection(radb.ast.Select(node.cond, r2), dd))

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

    return result