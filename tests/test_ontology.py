import json, unittest
from pathlib import Path
import yaml

ROOT=Path(__file__).resolve().parents[1]

class OntologyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ontology=yaml.safe_load((ROOT/'ontology'/'ontology.yaml').read_text())
        cls.manifest=json.loads((ROOT/'api'/'generated_manifest.json').read_text())
    def test_primary_keys_exist(self):
        for name,obj in self.ontology['objects'].items():
            self.assertIn(obj['primary_key'],obj['properties'],name)
    def test_links_reference_real_objects_and_properties(self):
        objs=self.ontology['objects']
        for name,link in self.ontology['links'].items():
            self.assertIn(link['from'],objs,name); self.assertIn(link['to'],objs,name)
            self.assertIn(link['from_property'],objs[link['from']]['properties'],name)
            self.assertIn(link['to_property'],objs[link['to']]['properties'],name)
    def test_actions_reference_real_objects(self):
        for action,spec in self.ontology['actions'].items():
            self.assertIn(spec['object'],self.ontology['objects'],action)
            for field in spec.get('set',{}):
                self.assertIn(field,self.ontology['objects'][spec['object']]['properties'])
    def test_codegen_manifest_matches_source(self):
        self.assertEqual(set(self.manifest['objects']),set(self.ontology['objects']))
        self.assertEqual(set(self.manifest['links']),set(self.ontology['links']))
        self.assertEqual(set(self.manifest['actions']),set(self.ontology['actions']))
    def test_dense_vector_mapping(self):
        mapping=json.loads((ROOT/'es'/'documents.json').read_text())
        self.assertEqual(mapping['mappings']['properties']['embedding']['type'],'dense_vector')
        self.assertEqual(mapping['mappings']['properties']['embedding']['dims'],384)

if __name__=='__main__': unittest.main()
