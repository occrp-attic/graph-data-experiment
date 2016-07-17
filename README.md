# leadgraph

Loading IJ data into a Neo4J instance. 


# Mapping file

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

# What queries

Looks like the harder part 

## Use case: African mining concessions

* Which company holds the most concessions across all datasets?
* Longest chains of ownership - 
* Can we track them back to Exhibit 21 structures, who is the BO?
* Can we make links to offshore datasets (PP, OL, BS, PA)?

## Use case: Moldovan linkages

* Small networks that have a large extent of control of Moldovan economy.
* Small networks connected to political actors (e.g. Parliament).
* Clusters within the larger economy
* Public contracts that connect to PEPs
* Public contracts that connect to the procurement blacklist

## Use case: PEPs and companies -- across all registers.

* Run all PEPs from EP & Aleph against all offshore registers and point
  out the ultimate children in an ownership chain.

## Use case: EU transparency data

* Show all advisory group member companies and persons that also 
  were awarded EU-wide contracts.
