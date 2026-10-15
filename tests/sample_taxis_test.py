from databricks_end_to_end_project import taxis


def test_find_all_taxis():
    results = taxis.find_all_taxis()
    assert results.count() > 5
