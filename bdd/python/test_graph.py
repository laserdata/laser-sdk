from pathlib import Path

from pytest_bdd import given, parsers, scenarios, then, when
from reference import GraphEngine

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "graph.feature"))


@given("an empty graph")
def open_graph(bench):
    bench.graph_engine = GraphEngine()


@when(parsers.parse('I observe "{src}" {edge_type} "{dst}"'))
def observe(bench, src, edge_type, dst):
    graph = bench.graph_engine
    from_id = graph.upsert_node(src)
    to_id = graph.upsert_node(dst)
    graph.add_edge(from_id, edge_type, to_id)


@when(parsers.parse('I observe "{src}" {edge_type} "{dst}" valid from {valid_from:d}'))
def observe_valid_from(bench, src, edge_type, dst, valid_from):
    graph = bench.graph_engine
    from_id = graph.upsert_node(src)
    to_id = graph.upsert_node(dst)
    graph.add_edge(from_id, edge_type, to_id, valid_from=valid_from)


@then(parsers.parse("the graph holds {count:d} nodes"))
def node_count(bench, count):
    assert bench.graph_engine.node_count() == count


@then(parsers.parse('traversing from "{start}" out "{first}" then "{second}" reaches "{target}"'))
def traverse_two_out(bench, start, first, second, target):
    reached = bench.graph_engine.traverse(start, [(first, "out"), (second, "out")])
    assert target in reached


@then(parsers.parse('traversing from "{start}" out "{edge}" reaches "{target}"'))
def traverse_out_reaches(bench, start, edge, target):
    assert target in bench.graph_engine.traverse(start, [(edge, "out")])


@then(parsers.parse('traversing from "{start}" out "{edge}" does not reach "{target}"'))
def traverse_out_excludes(bench, start, edge, target):
    assert target not in bench.graph_engine.traverse(start, [(edge, "out")])


@then(parsers.parse('traversing from "{start}" incoming "{edge}" reaches "{target}"'))
def traverse_in_reaches(bench, start, edge, target):
    assert target in bench.graph_engine.traverse(start, [(edge, "in")])


@then(parsers.parse('traversing from "{start}" out "{edge}" as of {at:d} reaches "{target}"'))
def traverse_as_of_reaches(bench, start, edge, at, target):
    assert target in bench.graph_engine.traverse(start, [(edge, "out")], as_of=at)


@then(
    parsers.parse('traversing from "{start}" out "{edge}" as of {at:d} does not reach "{target}"')
)
def traverse_as_of_excludes(bench, start, edge, at, target):
    assert target not in bench.graph_engine.traverse(start, [(edge, "out")], as_of=at)
