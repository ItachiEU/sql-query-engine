from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast
import radb.parse

'''
Control where the input data comes from, and where output data should go.
'''
class ExecEnv(Enum):
    LOCAL = 1   # read/write local files
    HDFS = 2    # read/write HDFS
    MOCK = 3    # read/write mock data to an in-memory file system.

'''
Switches between different execution environments and file systems.
'''
class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)
    
    def get_output(self, fn):
        if self.exec_environment == ExecEnv.HDFS:
            return luigi.contrib.hdfs.HdfsTarget(fn)
        elif self.exec_environment == ExecEnv.MOCK:
            return MockTarget(fn)
        else:
            return luigi.LocalTarget(fn)


class InputData(OutputMixin):
    filename = luigi.Parameter()

    def output(self):
        return self.get_output(self.filename)


'''
Counts the number of steps / luigi tasks that we need for evaluating this query.
'''
def count_steps(raquery):
    assert(isinstance(raquery, radb.ast.Node))

    if (isinstance(raquery, radb.ast.Select) or isinstance(raquery,radb.ast.Project) or
        isinstance(raquery,radb.ast.Rename)):
        return 1 + count_steps(raquery.inputs[0])

    elif isinstance(raquery, radb.ast.Join):
        return 1 + count_steps(raquery.inputs[0]) + count_steps(raquery.inputs[1])

    elif isinstance(raquery, radb.ast.RelRef):
        return 1

    else:
        raise Exception("count_steps: Cannot handle operator " + str(type(raquery)) + ".")


class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):
    '''
    Each physical operator knows its (partial) query string.
    As a string, the value of this parameter can be searialized
    and shipped to the data node in the Hadoop cluster.
    '''
    querystring = luigi.Parameter()

    '''
    Each physical operator within a query has its own step-id.
    This is used to rename the temporary files for exhanging
    data between chained MapReduce jobs.
    '''
    step = luigi.IntParameter(default=1)

    '''
    In HDFS, we call the folders for temporary data tmp1, tmp2, ...
    In the local or mock file system, we call the files tmp1.tmp...
    '''
    def output(self):
        if self.exec_environment == ExecEnv.HDFS:
            filename = "tmp" + str(self.step)
        else:
            filename = "tmp" + str(self.step) + ".tmp"
        return self.get_output(filename)


'''
Given the radb-string representation of a relational algebra query,
this produces a tree of luigi tasks with the physical query operators.
'''
def task_factory(raquery, step=1, env=ExecEnv.HDFS):
    assert(isinstance(raquery, radb.ast.Node))
    
    if isinstance(raquery, radb.ast.Select):
        return SelectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.RelRef):
        filename = raquery.rel + ".json"
        return InputData(filename=filename, exec_environment=env)

    elif isinstance(raquery, radb.ast.Join):
        return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(querystring=str(raquery) + ";", step=step, exec_environment=env)
                          
    else:
        # We will not evaluate the Cross product on Hadoop, too expensive.
        raise Exception("Operator " + str(type(raquery)) + " not implemented (yet).")
    

class JoinTask(RelAlgQueryTask):

    def requires(self):
        raquery: radb.ast.Join = radb.parse.one_statement_from_string(self.querystring)
        assert(isinstance(raquery, radb.ast.Join))
      
        task1 = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)
        task2 = task_factory(raquery.inputs[1], step=self.step + count_steps(raquery.inputs[0]) + 1, env=self.exec_environment)

        return [task1, task2]

    
    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery:radb.ast.Join = radb.parse.one_statement_from_string(self.querystring)
        condition = raquery.cond

        join_attributes = []

        if isinstance(condition, radb.ast.ValExprBinaryOp) and condition.op == radb.parse.RAParser.EQ:
            left = condition.inputs[0]
            right = condition.inputs[1]
            if isinstance(left, radb.ast.AttrRef) and isinstance(right, radb.ast.AttrRef):
                join_attributes.append((left.name, right.name))

        if join_attributes:
            for left_attr, right_attr in join_attributes:
                relation_name = relation.split('.')[0]
                key = json_tuple[left_attr if relation_name == left_attr.split('.')[0] else right_attr]
                value = (relation, {k: v for k, v in json_tuple.items() if k != (left_attr if relation_name == left_attr.split('.')[0] else right_attr)})
                yield key, json.dumps(value)

    def reducer(self, key, values):
        raquery: radb.ast.Join = radb.parse.one_statement_from_string(self.querystring)

        tuples_from_R = []
        tuples_from_S = []

        for value in values:
            relation, tuple = json.loads(value)
            relation_name = relation.split('.')[0]
            left_attr = raquery.cond.inputs[0].rel.split('.')[0]
            if relation_name == left_attr:
                tuples_from_R.append(tuple)
            else:
                tuples_from_S.append(tuple)

        seen_tuples = set()
        for tuple_R in tuples_from_R:
            for tuple_S in tuples_from_S:
                result_tuple = {**tuple_R, **tuple_S}
                result_tuple_json = json.dumps(result_tuple, sort_keys=True)

                if result_tuple_json not in seen_tuples:
                    seen_tuples.add(result_tuple_json)
                    yield 'R_S', result_tuple_json


class SelectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert(isinstance(raquery, radb.ast.Select))
        
        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    
    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        condition: radb.ast.Select = radb.parse.one_statement_from_string(self.querystring).cond
        
        def evaluate_condition(condition, json_tuple):
            if isinstance(condition, radb.ast.AttrRef):
                return json_tuple[f"{relation}.{condition.name}"]
            elif isinstance(condition, radb.ast.ValExprBinaryOp):
                left = evaluate_condition(condition.inputs[0], json_tuple)
                right = evaluate_condition(condition.inputs[1], json_tuple)

                if condition.op == radb.parse.RAParser.EQ:
                    return left == right
                elif condition.op == radb.parse.RAParser.AND:
                    return left and right
            elif isinstance(condition, radb.ast.ValExpr):
                if isinstance(condition, radb.ast.RAString):
                    return condition.val.strip("'")
                if isinstance(condition, radb.ast.RANumber):
                    return int(condition.val)
                return condition.val
            else:
                raise Exception("Unsupported condition type: {}".format(type(condition)))

        if evaluate_condition(condition, json_tuple):
            yield (relation, json.dumps(json_tuple))

class RenameTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert(isinstance(raquery, radb.ast.Rename))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]


    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery: radb.ast.Rename = radb.parse.one_statement_from_string(self.querystring)

        renamed_tuple = json_tuple.copy()
        if raquery.relname is not None:
            new_relation = raquery.relname
            for k, v in json_tuple.items():
                renamed_tuple[k.replace(relation, new_relation)] = renamed_tuple.pop(k)
        if raquery.attrnames is not None:
            for old_attr, new_attr in zip(raquery.inputs[0].type.attrs, raquery.attrnames):
                renamed_tuple[new_attr] = renamed_tuple.pop(old_attr.name)
        yield (new_relation, json.dumps(renamed_tuple))


class ProjectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert(isinstance(raquery, radb.ast.Project))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]    

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        attrs: list[radb.ast.ValExpr] = radb.parse.one_statement_from_string(self.querystring).attrs
        attr_names: list[str] = [attr.name for attr in attrs]

        projected_tuple = {
            k: v for k, v in json_tuple.items() 
            if k.split('.')[1] in attr_names and k.split('.')[0] == relation
        }
        yield (relation, json.dumps(projected_tuple))
        

    def reducer(self, key, values):

        unique_tuples = set(values)
        for value in unique_tuples:
            yield (key, value)

        
if __name__ == '__main__':
    luigi.run()