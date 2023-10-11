# Modern Data Stack Orchestration Comparison
A small project to compare common orchestrators used in Data Engineering. The test is to do a simple ELT Pipeline that takes data from a public API, stores it locally to a file, and then loads it to a local sqlite database.

# Limitations
Issues relating to how the orchestrator runs on infrastructure will not be tested as all the tests will be *benchmarked* locally. Also issues that tend to only arise in production environments and at scale will likely not be a consideration.

# Focus
The focus here will be to test how workflows are defined and executed, and not the ease of building tasks. The point of the project is to compare the tools as orchestrators, not whole data pipeline solutions. I believe to build effective data pipelines several tools should be composed together.

# To Do
- [ ] Airflow
- [ ] Dagster
- [ ] Prefect
- [ ] Temporal
- [ ] Specify Dependencies
