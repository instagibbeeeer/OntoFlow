import os
import requests
from . import generated_models

class ActionProxy:
    def __init__(self, client, obj): self.client,self.obj=client,obj
    def __getattr__(self,name):
        suffix=self.obj.__class__.__name__.replace('QualityFinding','finding').replace('MaintenanceAction','maintenance_action').lower()
        candidates=[name, f'{name}_{suffix}']
        manifest=self.client.manifest
        action=next((a for a in candidates if a in manifest.get('actions',{})),None)
        if not action: raise AttributeError(f'No action {name} for {self.obj.__class__.__name__}')
        return lambda **kwargs:self.client.apply_action(action,self.obj,**kwargs)

class OntologyObjectMixin:
    @property
    def actions(self): return ActionProxy(self._client,self)

class ObjectSet:
    def __init__(self,client,name,filters=None): self.client,self.name,self.filters=client,name,filters or {}
    def get(self,object_id): return self.client.get(self.name,object_id)
    def where(self,**kwargs):
        f=dict(self.filters); f.update(kwargs); return ObjectSet(self.client,self.name,f)
    def all(self,limit=50): return self.client.query(self.name,self.filters,limit)

class OntoFlowClient:
    def __init__(self,base_url='http://localhost:8000'):
        self.base_url=base_url.rstrip('/')
        self.manifest=self._load_manifest()
    def _load_manifest(self):
        from pathlib import Path
        import json
        p=Path(__file__).resolve().parents[2]/'api'/'generated_manifest.json'
        return json.loads(p.read_text()) if p.exists() else {'actions':{}}
    def __getattr__(self,name):
        if hasattr(generated_models,name): return ObjectSet(self,name)
        raise AttributeError(name)
    def _materialize(self,name,data):
        if data is None:return None
        cls=getattr(generated_models,name); values={k:data.get(k) for k in cls.__dataclass_fields__ if k not in ('_client','_id')}; values['_id']=data.get('id') or data.get('_id'); obj=cls(**values)
        obj._client=self
        obj.__class__.actions=OntologyObjectMixin.actions
        return obj
    def get(self,name,object_id):
        r=requests.get(f'{self.base_url}/objects/{name}/{object_id}',timeout=10); r.raise_for_status(); return self._materialize(name,r.json())
    def query(self,name,filters=None,limit=50):
        r=requests.post(f'{self.base_url}/objects/{name}/query',json={'filters':filters or {},'limit':limit},timeout=10); r.raise_for_status(); return [self._materialize(name,x) for x in r.json()]
    def traverse(self,obj,link_name):
        name=obj.__class__.__name__; spec=self.manifest['objects'][name]; oid=getattr(obj,spec['primary_key'])
        r=requests.get(f'{self.base_url}/objects/{name}/{oid}/links/{link_name}',timeout=10); r.raise_for_status(); data=r.json()
        target=None
        if link_name in self.manifest['links']: target=self.manifest['links'][link_name]['to']
        else:
            for _,link in self.manifest['links'].items():
                if link.get('reverse_name')==link_name and link['to']==name: target=link['from']; break
        return [self._materialize(target,x) for x in data]
    def apply_action(self,action,obj,**inputs):
        spec=self.manifest['objects'][obj.__class__.__name__]; oid=getattr(obj,spec['primary_key'])
        r=requests.post(f'{self.base_url}/actions/{action}/{oid}',json={'inputs':inputs},timeout=10); r.raise_for_status(); return self._materialize(obj.__class__.__name__,r.json())
    def rag(self,query,dq_min=None):
        r=requests.post(f'{self.base_url}/rag',json={'query':query,'dq_min':dq_min},timeout=30); r.raise_for_status(); return r.json()
