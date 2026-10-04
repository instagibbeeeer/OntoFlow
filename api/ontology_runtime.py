from pathlib import Path
import json

_MANIFEST = Path(__file__).with_name('generated_manifest.json')
with open(_MANIFEST, encoding='utf-8') as f:
    ONTOLOGY = json.load(f)

OBJECTS = ONTOLOGY['objects']
LINKS = ONTOLOGY['links']
ACTIONS = ONTOLOGY['actions']
CONSTRAINTS = ONTOLOGY.get('constraints', {})


def object_spec(name):
    if name not in OBJECTS:
        raise KeyError(f'Unknown ontology object type: {name}')
    return OBJECTS[name]


#def outgoing_links(name):
#    return {k:v for k,v in LINKS.items() if v['from']==name}
#
#
#def incoming_links(name):
#    return {v.get('reverse_name', k): (k,v) for k,v in LINKS.items() if v['to']==name and v.get('reverse_name')}
#
#
#def action_for_object(name):
#    return {k:v for k,v in ACTIONS.items() if v['object']==name}
