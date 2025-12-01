import os
import csv
import tempfile

from tools import pseudonymisation_excel as pe
from pseudonymisation_cnps_anstat import generer_id_anstat


def test_generate_mapping_and_write_csv():
    key = 'testkey123'
    os.environ['ANSTAT_SECRET_KEY'] = key

    # create a small input CSV
    fd_in, path_in = tempfile.mkstemp(suffix='.csv')
    os.close(fd_in)
    with open(path_in, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh)
        writer.writerow(['cnps'])
        writer.writerow(['194011724471'])
        writer.writerow(['194011724472'])
        writer.writerow(['194011724471'])  # duplicate should be deduped

    # generate mapping
    values = list(pe.read_values_from_file(path_in, 'cnps'))
    assert len(values) == 3

    mapping = list(pe.generate_mapping(values, key))
    # should dedupe to 2 unique originals
    assert len(mapping) == 2
    origs = [r[0] for r in mapping]
    assert '194011724471' in origs and '194011724472' in origs

    fd_out, path_out = tempfile.mkstemp(suffix='.csv')
    os.close(fd_out)
    pe.write_mapping_csv(path_out, mapping)

    # read back and verify content
    with open(path_out, newline='', encoding='utf-8') as fh:
        rdr = csv.DictReader(fh)
        rows = list(rdr)
    assert len(rows) == 2
    for r in rows:
        orig = r['original']
        assert r['pseudonyme'] == generer_id_anstat(orig, key)
