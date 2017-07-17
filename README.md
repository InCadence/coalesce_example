This is a basic example program showing how to use the thick client APIs to communicate with Coalesce.
The program illustrates:
  1. Creating Entities and storing them
  2. Creating templates and registering them for entities
  3. Searching for and updating existing entities
  4. Linking entities
  5. Persisting to multiple persisters

This program is currently configured to communicate with the Accumulo and Neo4J persisters.
The persisters to use and the connection information for each persister can be set through the configuration
files in the conf directory.

To run this program:
  1. Clone this git repository
  2. Build in Eclipse or with maven
  3. Run as the script provided.
