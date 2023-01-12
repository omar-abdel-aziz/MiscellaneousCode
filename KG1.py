
from pydantic import BaseModel
from neo4j import GraphDatabase

from typing import Optional, Dict, List

# This class connects to a Neo4j database and runs queries.
class db_connect:
    # The constructor makes a connection to the database using a URI, username, and password.
    def __init__(self, uri, user, pwd):
        self.__driver = GraphDatabase.driver(uri, auth=(user, pwd), encrypted=False)

    # The close method closes the connection to the database.
    def close(self):
        self.__driver.close()

    # The query method runs a query on the database and returns the result.
    def query(self, query, db=None):
        session = self.__driver.session(database=db) if db else self.__driver.session()
        try:
            response = list(session.run(query))
        except Exception as e:
            print("The query could not be completed:", e)
        finally:
            session.close()
        return response
# This model represents the parameters for an edge in a Neo4j graph.
class EdgeParams(BaseModel):
    # The subject of the edge.
    subject: str
    # The object of the edge.
    object: str
    # The predicates of the edge.
    predicates: List[str]
    # The attributes of the edge (optional).
    attributes: Optional[Dict[str, str]]

# This model represents the parameters for a node in a Neo4j graph.
class NodeParams(BaseModel):
    # The categories of the node.
    categories: List[str]
    # The IDs of the node (optional).
    ids: Optional[List[str]]

# This model represents the parameters for a query to a Neo4j graph.
class QueryGraph(BaseModel):
    # The edges in the query.
    edges: Dict[str, EdgeParams]
    # The nodes in the query.
    nodes: Dict[str, NodeParams]

# This model represents a message containing a query for a Neo4j graph.
class QueryMessage(BaseModel):
    # The query for the Neo4j graph.
    query_graph: QueryGraph
    
# This model represents a query to a Neo4j graph.
class Query(BaseModel):
    # The message containing the query for the Neo4j graph.
    message: QueryMessage
###
###
def parse_query(query:Query):
    # Initialize result dictionary
    result = {}

    # Declare variables for nodes and edges in the query graph
    nodes = Dict[str, EdgeParams]
    edges = Dict[str, EdgeParams]

    # Assign nodes and edges from the query graph
    nodes = query.message.query_graph.nodes
    edges = query.message.query_graph.edges

    # Extract edge parameters for edge "e00"
    e00: EdgeParams = edges['e00']

    # Initialize empty lists for predicates and properties
    e00_predicates = []
    e00_property = []

    # Initialize empty strings for predicate type, property type, and property value
    e00_predicate_type = ''
    e00_property_type = ''
    e00_property_value = ''

    # Initialize empty strings for subject and object nodes
    subject_node = ''
    object_node = ''

    # Assign list of predicates and subject/object nodes
    e00_predicates = e00.predicates 
    subject_node = e00.subject
    object_node = e00.object

    # Extract predicate type from first predicate in the list and convert to uppercase
    e00_predicate_type = e00_predicates[0].split(':')[1].upper()
#-------------------------------------------------------------------------------------------#
# for future code implementation where queries are set up to use the edge attributes
    # If the edge has attributes
    # if e00.attributes is not None:
    #     # Assign attributes to e00_property
    #     e00_property = e00.attributes 

    #     # Extract property type and value from attributes
    #     e00_property_type = e00_property.split(':')[0].split(':')[1].upper() 
    #     e00_property_value = e00_property.split(':')[1]
#-------------------------------------------------------------------------------------------#
# Handle node n00
    subject: NodeParams = nodes[subject_node]  # Extract node parameters for subject node
    subject_categories = []                    # Initialize empty list for subject categories
    subject_category_type = ''                 # Initialize empty string for subject category type
    subject_ids = []                           # Initialize empty list for subject IDs
    subject_property_type = ''                 # Initialize empty string for subject property type
    subject_property_value = ''                # Initialize empty string for subject property value

    # Assign list of subject categories and extract subject category type
    subject_categories = subject.categories 
    subject_category_type = subject_categories[0].split(':')[1] 

    # If the subject node has IDs
    if subject.ids is not None:
        # Assign list of IDs to subject_ids
        subject_ids = subject.ids 
        # Iterate through the IDs
        for id in subject_ids:
            # Extract property type and value from ID
            subject_property_type = id.split(':')[0] 
            subject_property_value = id.split(':')[1] 

        # If the property type is not "Symbol" or "Name", add "_ID" to the property type and convert the property value to an integer
        if (subject_property_type != 'Symbol') & (subject_property_type != 'Name'):
            subject_property_type = subject_property_type + "_ID"
            subject_property_value = int(subject_property_value)
        # Otherwise, convert the property value to uppercase
        else:
            subject_property_value = subject_property_value.upper()

# Handle node n01
    object: NodeParams = nodes[object_node]    # Extract node parameters for object node
    object_categories = []                     # Initialize empty list for object categories
    object_category_type = ''                  # Initialize empty string for object category type
    object_ids = []                            # Initialize empty list for object IDs
    object_property_type = ''                  # Initialize empty string for object property type
    object_property_value = ''                 # Initialize empty string for object property value

    # Assign list of object categories and extract object category type
    object_categories = object.categories 
    object_category_type = object_categories[0].split(':')[1] 

    # If the object node has IDs
    if object.ids is not None:
        # Assign list of IDs to object_ids
        object_ids = object.ids 
        # Iterate through the IDs
        for id in object_ids:
            # Extract property type and value from ID
            object_property_type = id.split(':')[0]
            object_property_value = id.split(':')[1]

        # If the property type is not "Symbol" or "Name", add "_ID" to the property type and convert the property value to an integer
        if (object_property_type != 'Symbol') & (object_property_type != 'Name'):
            object_property_type = object_property_type + "_ID"
            object_property_value = int(object_property_value)
        # Otherwise, convert the property value to uppercase
        else:
            object_property_value = object_property_value.upper()

# Concatenate strings with the extracted values
    string1 = 'n00:' + subject_category_type
    string2 = 'e00:' + e00_predicate_type
    string3 = 'n01:' + object_category_type
    string4 = 'n00.' + subject_property_type + '=' + subject_property_value
    string5 = 'n01.' + object_property_type + '=' + object_property_value

# The resulting query will have the following form:
# MATCH (n00:subject_category_type)-[e00:e00_predicate_type]-(n01:object_category_type)
# WHERE n00.subject_property_type=subject_property_value AND n01.object_property_type=object_property_value
# RETURN DISTINCT n00, e00, n01

# For example:
# MATCH (n00:Drug)-[e00:TARGETS]-(n01:Gene)
# WHERE n00.Name="PACLITAXEL" AND n01.Symbol="BCL2"
# RETURN DISTINCT n00, e00, n01

# Assign the concatenated strings to the result dictionary
    result= {"string1": string1, "string2": string2, "string3": string3, "string4": string4,"string5": string5}

# Return the result dictionary
    return(result)

def query_KG(json_query,db,string1,string2,string3,string4,string5):
    # If both string4 and string5 are not empty
    if (string4 != 'n00.=') & (string5 != 'n01.='):
        # Construct a query with all three WHERE clauses
        query = ''' MATCH ({string1})-[{string2}]-({string3}) WHERE {string4} AND {string5} RETURN DISTINCT n00, e00, n01, type(e00)'''.format(string1=string1,string2=string2,string3=string3,string4=string4,string5=string5)
    
    # If string4 is empty
    elif (string4 == 'n00.='):
        # Construct a query with only the second and third WHERE clauses
        query = '''MATCH ({string1})-[{string2}]-({string3}) WHERE {string5} RETURN DISTINCT n00, e00, n01, type(e00)'''.format(string1=string1,string2=string2,string3=string3,string5=string5)
    
    # If string5 is empty
    elif (string5 == 'n01.='):
        # Construct a query with only the first and second WHERE clauses
        query = '''MATCH ({string1})-[{string2}]-({string3}) WHERE {string4} RETURN DISTINCT n00, e00, n01, type(e00)'''.format(string1=string1,string2=string2,string3=string3,string4=string4)
    
    # If both string4 and string5 are empty
    else:
        # Assign an empty string to the query
        query = ''''''

    # Execute the query and assign the result to a variable
    result = db.query(query, db='neo4j')

    # Initialize an empty list to store the results
    response_list = []

# Iterate over the results of the query
    for word in result:
        # Initialize an empty dictionary to store the response message
        response_message={}

        # Assign the input query to the response message
        response_message["query_graph"] = json_query
        # Initialize an empty dictionary for the results field
        response_message["results"] = {}
        # Initialize an empty dictionary for the knowledge graph field
        response_message["knowledge_graph"] =  {}
        # Initialize an empty dictionary for the edges field
        response_message["knowledge_graph"]["edges"] =  {}
        # Initialize an empty dictionary for the nodes field
        response_message["knowledge_graph"]["nodes"] =  {}

        # Convert the word to a dictionary
        w = dict(word)
        # Get the n00, e00, and n01 fields from the dictionary
        n0 = dict(w.get("n00"))
        e0 = dict(w.get("e00"))
        n1 = dict(w.get("n01"))
        # Get the type of the edge
        predicate = w.get("type(e00)")

    # Assign the values to the appropriate fields in the response message
        response_message['knowledge_graph']['nodes']["n00"] = {
                                                            "Subject_Name": n0.get("Name"),
                                                            "Subject_Symbol": n0.get("Symbol"),
                                                            "Subject_Category": n0.get("Category"),
                                                            "Subject_attributes": 
                                                                {
                                                                "Subject_NCBI_ID": n0.get("NCBI_ID"),
                                                                "Subject_MONDO_ID": n0.get("MONDO_ID"),
                                                                "Subject_Chembl_ID": n0.get("Chembl_ID"),
                                                                "Subject_Pubchem_ID": n0.get("Pubchem_ID"),
                                                                "Subject_Prefixes": n0.get("Prefixes")
                                                                }
                                                            }

        response_message['knowledge_graph']['edges']["e00"] = {
                                                            "Predicate": predicate,
                                                            "Edge_attributes": 
                                                                {
                                                                "Edge_attribute_publications": e0.get("Publications"),
                                                                "Edge_attribute_knowledge_source": e0.get("Knowledge_Source"),
                                                                "Edge_attribute_provided_by": e0.get("Provided_by"),
                                                                "Edge_attribute_FDA_approval_status": e0.get("FDA_approval_status")
                                                                }
                                                            }

        response_message['knowledge_graph']['nodes']["n01"] = {
                                                            "Object_Name": n1.get("Name"),
                                                            "Object_Symbol": n1.get("Symbol"),
                                                            "Object_Category": n1.get("Category"),
                                                            "Object_attributes": 
                                                                {
                                                                "Object_NCBI_ID": n1.get("NCBI_ID"),
                                                                "Object_MONDO_ID": n1.get("MONDO_ID"),
                                                                "Object_Chembl_ID": n1.get("Chembl_ID"),
                                                                "Object_Pubchem_ID": n1.get("Pubchem_ID"),
                                                                "Object_Synonym": n1.get("Synonym"),
                                                                "Object_Prefixes": n1.get("Prefixes")
                                                                }
                                                            }
        # Insert the response message into the list of results
        response_list.insert(len(response_list)-1, response_message)

    response = {}
    response["message"] = response_list
    return(response)
def Query_KG_all(json_query, db):
    #Queries the knowledge graph using the input query and returns the results.
    result = parse_query(json_query)
    df = query_KG(json_query, db, result['string1'], result['string2'], result['string3'], result['string4'], result['string5'])
    return df

