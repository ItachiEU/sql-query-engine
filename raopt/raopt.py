from typing import Dict, List, Set

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
 
def get_condition_attributes(cond: radb.ast.Node):
    if isinstance(cond, radb.ast.AttrRef):
        yield cond
    else:
        for child in cond.inputs:
            yield from get_condition_attributes(child) 

def extract_attributes_from_cross(node: radb.ast.Node, dd: Dict[str, Dict[str, str]]) -> Set[str]:
    if isinstance(node, radb.ast.RelRef):
        return set(dd[node.rel].keys())

    elif isinstance(node, radb.ast.Cross):
        left_attrs = extract_attributes_from_cross(node.inputs[0], dd)
        right_attrs = extract_attributes_from_cross(node.inputs[1], dd)
        return left_attrs.union(right_attrs)

    elif isinstance(node, radb.ast.Rename):
        original_relname = node.inputs[0].rel
        if node.relname:
            original_attrs = dd[original_relname]
            if node.attrnames:
               return set(node.attrnames)
            else:
               return set(original_attrs.keys())

    else:
        raise ValueError(f"Unexpected node type encountered while extracting attributes: {type(node)}")

def rule_push_down_selections(node: radb.ast.Node, dd: Dict[str, Dict[str, str]]) -> radb.ast.Node:
    if isinstance(node, radb.ast.Select):
        child_node = rule_push_down_selections(node.inputs[0], dd)

        if isinstance(child_node, radb.ast.Cross):
            r1, r2 = child_node.inputs
            attributes = {attr.name for attr in get_condition_attributes(node.cond)}

            r1_attrs = extract_attributes_from_cross(r1, dd)
            print("r1 attrs: ", r1_attrs)
            r2_attrs = extract_attributes_from_cross(r2, dd)
            print("r2 attrs: ", r2_attrs)

            if set(attributes).issubset(r1_attrs) and not set(attributes).intersection(r2_attrs):
                return radb.ast.Cross(rule_push_down_selections(radb.ast.Select(node.cond, r1), dd), r2)

            if set(attributes).issubset(r2_attrs) and not set(attributes).intersection(r1_attrs):
                return radb.ast.Cross(r1, rule_push_down_selections(radb.ast.Select(node.cond, r2), dd))

        return radb.ast.Select(node.cond, child_node)

    elif isinstance(node, radb.ast.Project):
        return radb.ast.Project(node.attrs, rule_push_down_selections(node.inputs[0], dd))

    elif isinstance(node, radb.ast.Cross):
        return radb.ast.Cross(rule_push_down_selections(node.inputs[0], dd), rule_push_down_selections(node.inputs[1], dd))

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

        return radb.ast.Rename(node.relname, node.attrnames, rule_push_down_selections(node.inputs[0], updated_dd))


    else:
        raise ValueError("Unexpected node type encountered.")
