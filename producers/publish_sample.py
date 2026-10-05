import json
from kafka import KafkaProducer

p=KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda x:json.dumps(x).encode())
objects=[
 ('raw.events',{'object_type':'Machine','machine_id':'M1','name':'Welding Robot 17','status':'running','production_line':'Body Shop A','source_system':'MES'}),
 ('raw.events',{'object_type':'Part','part_id':'P-456','name':'Door reinforcement','revision':'C','source_system':'PLM'}),
 ('raw.events',{'object_type':'Order','order_id':'ORD-001','product':'Vehicle X','status':'in_production','source_system':'ERP'}),
 ('raw.events',{'object_type':'Batch','batch_id':'B123','machine_id':'M1','order_id':'ORD-001','product':'Vehicle X','started_at':'2026-09-30T10:00:00Z','source_system':'MES'}),
 ('raw.events',{'object_type':'QualityFinding','finding_id':'QF-123','batch_id':'B123','part_id':'P-456','machine_id':'M1','title':'Dimensional deviation','description':'Door reinforcement measured outside tolerance at station S1.','severity':'high','status':'open','timestamp':'2026-09-30T10:15:00Z','source_system':'QA'}),
 ('raw.events',{'object_type':'MaintenanceAction','maintenance_id':'MA-42','machine_id':'M1','action_type':'replace_tool','description':'Replaced worn welding fixture tool.','timestamp':'2026-09-30T10:25:00Z','source_system':'CMMS'}),
 ('raw.documents',{'object_type':'EngineeringDocument','document_id':'DOC-17','title':'Batch B123 QA report','text':'Batch B123 had a high-severity dimensional deviation on part P-456 while running on machine M1. Inspection found fixture wear. Tool replacement MA-42 was completed shortly afterwards.','batch_id':'B123','machine_id':'M1','part_id':'P-456','source_system':'QA_REPORT','dq_score':0.96,'valid':True})
]
for topic,obj in objects: p.send(topic,obj)
p.flush(); print(f'Published {len(objects)} ontology objects.')
