"""Pure selection and summary contracts; no Docker or expensive benchmark."""
from types import SimpleNamespace

from experiments.explainability.treeshap.iforest_tree_vs_kernel import select_events, summarize


def events():
    return [SimpleNamespace(identity=f'{group}-{i}', group=group, offset=i+100*g,
                            ordinal=i, generation=1 if i<=10 else 2, refit=i==10, score=i/20)
            for g,group in enumerate(['a','b']) for i in range(20)]


def test_short_contains_refit_boundaries_and_highest():
    selected = select_events(events(), 'short')
    assert {(e.group,e.ordinal) for e in selected} == {(g,i) for g in ['a','b'] for i in [9,10,11,19]}
    assert [e.identity for e in selected] == [e.identity for e in select_events(events(),'short')]


def test_full_stratification_is_deterministic_and_preserves_mandatory():
    selected = select_events(events(),'full',20)
    assert len(selected)==20
    assert len({e.identity for e in selected})==20
    assert {e.identity for e in select_events(events(),'short')} <= {e.identity for e in selected}


def test_summary_does_not_mix_methods_or_rank_and_value_errors():
    rows = [dict(method='TREE-int',elapsed_ms=2,metrics={'rmse':0}),
            dict(method='K-prod',elapsed_ms=10,metrics={'spearman':.5})]
    summary = summarize(rows,[1])
    assert summary['methods']['TREE-int']['median_ms']==2
    assert 'rmse' not in summary['methods']['K-prod']['metrics']
    assert summary['tree_conversion_amortized_ms']==1
