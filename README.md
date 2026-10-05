Ontology-as-Code, event-drivenindustrial data platform inspired by the architectural ideas presented in Fraunhofer’s research on ontology-based microservice architectures for industrial assistance systems with SDK, API and RAG downstreams from an ontology language with the goal of turning it into a no code workflow creation platform with pro-code under the hood transform creation. It turns messy data into structured, searchable, linked, and AI-ready information. 

The project follows the same core principle: production data should be connected through a shared simple semantic model rather than handled as isolated system-specific data.

<img width="1375" height="1020" alt="graph" src="https://github.com/user-attachments/assets/ababbf93-b38d-4a4d-943e-0bf00d163f8f" />


OntoFlow extends this idea with modern streaming and AI-oriented data architecture. Data from sources such as production systems, quality systems, maintenance applications and technical documents is ingested through Apache Kafka and processed using Apache Spark Structured Streaming. Data is persisted through a Delta Lake Bronze–Silver–Gold architecture, where raw records are preserved, validated and transformed into typed **ontology objects**.

Elasticsearch provides structured filtering, full-text search and vector retrieval, while FastAPI and GraphQL expose ontology objects and relationships to applications. 

OntoFlow also includes ontology-aware hybrid RAG, combining BM25 search, vector similarity and relationship traversal. This enables AI applications to retrieve not only documents containing matching words, but also connected engineering context such as the machine, batch, quality finding, maintenance action and technical documentation associated with a production issue.

Technologies: Python, Apache Kafka, PySpark, Delta Lake, Elasticsearch, FastAPI, GraphQL, Docker.
