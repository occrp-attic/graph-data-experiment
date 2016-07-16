# leadgraph

Loading IJ data into a Neo4J instance. 


### Mapping file

```yaml
## Database configuration URL:
# Can also be defined as DATABASE_URI in the environment.
database: postgresql://localhost/database

## Neo4J (destination DB) configuration:
# Can also be defined as NEO4J_HOST, NEO4J_USER, NEO4J_PASSWORD
# in the application's environment.
graph_host: http://localhost:7474
graph_user: neo4j
graph_password: neo4j
```
