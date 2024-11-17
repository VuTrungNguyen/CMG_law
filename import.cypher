:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  // file_path_root: 'file:///', // Change this to the folder your script can access the files at.
  file_0: 'https://github.com/VuTrungNguyen/CMG_law/blob/bfe8f6730524e0190a659d211c3cda2c26ac1efb/Civil%20Codes.csv',
  file_1: 'https://github.com/VuTrungNguyen/CMG_law/blob/bfe8f6730524e0190a659d211c3cda2c26ac1efb/normalized_cases.csv'
};

// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
// NOTE: The following constraint creation syntax is generated based on the current connected database version 5.25-aura.
CREATE CONSTRAINT `Civil_Code ID_Civil_Code_ID_uniq` IF NOT EXISTS
FOR (n: `Civil_Code_ID`)
REQUIRE (n.`Civil Code ID`) IS UNIQUE;
CREATE CONSTRAINT `Inheritance_Code_Inheritance_Code_uniq` IF NOT EXISTS
FOR (n: `Inheritance_Code`)
REQUIRE (n.`Inheritance Code`) IS UNIQUE;
CREATE CONSTRAINT `Court_Finding_Court_Finding_uniq` IF NOT EXISTS
FOR (n: `Court_Finding`)
REQUIRE (n.`Court Finding`) IS UNIQUE;
CREATE CONSTRAINT `Court_Opinion_Court_Opinion_uniq` IF NOT EXISTS
FOR (n: `Court_Opinion`)
REQUIRE (n.`Court Opinion`) IS UNIQUE;
CREATE CONSTRAINT `Court_Judgment_Court_Judgment_uniq` IF NOT EXISTS
FOR (n: `Court_Judgment`)
REQUIRE (n.`Court Judgment`) IS UNIQUE;
CREATE CONSTRAINT `Case_Number_Case_Number_uniq` IF NOT EXISTS
FOR (n: `Case_Number`)
REQUIRE (n.`Case Number`) IS UNIQUE;

:param {
  idsToSkip: []
};

// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
LOAD CSV WITH HEADERS FROM ( $file_0) AS row
WITH row
WHERE NOT row.`Civil Code ID` IN $idsToSkip AND NOT toInteger(trim(row.`Civil Code ID`)) IS NULL
CALL {
  WITH row
  MERGE (n: `Civil_Code_ID` { `Civil Code ID`: toInteger(trim(row.`Civil Code ID`)) })
  SET n.`Civil Code ID` = toInteger(trim(row.`Civil Code ID`))
  SET n.`civil_code_chinese` = row.`Civil Code Chinese Number`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ( $file_0) AS row
WITH row
WHERE NOT row.`Inheritance Code` IN $idsToSkip AND NOT row.`Inheritance Code` IS NULL
CALL {
  WITH row
  MERGE (n: `Inheritance_Code` { `Inheritance Code`: row.`Inheritance Code` })
  SET n.`Inheritance Code` = row.`Inheritance Code`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ( $file_1) AS row
WITH row
WHERE NOT row.`Court Finding` IN $idsToSkip AND NOT row.`Court Finding` IS NULL
CALL {
  WITH row
  MERGE (n: `Court_Finding` { `Court Finding`: row.`Court Finding` })
  SET n.`Court Finding` = row.`Court Finding`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ( $file_1) AS row
WITH row
WHERE NOT row.`Court Opinion` IN $idsToSkip AND NOT row.`Court Opinion` IS NULL
CALL {
  WITH row
  MERGE (n: `Court_Opinion` { `Court Opinion`: row.`Court Opinion` })
  SET n.`Court Opinion` = row.`Court Opinion`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ( $file_1) AS row
WITH row
WHERE NOT row.`Court Judgment` IN $idsToSkip AND NOT row.`Court Judgment` IS NULL
CALL {
  WITH row
  MERGE (n: `Court_Judgment` { `Court Judgment`: row.`Court Judgment` })
  SET n.`Court Judgment` = row.`Court Judgment`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ( $file_1) AS row
WITH row
WHERE NOT row.`Case Number` IN $idsToSkip AND NOT toFloat(trim(row.`Case Number`)) IS NULL
CALL {
  WITH row
  MERGE (n: `Case_Number` { `Case Number`: toFloat(trim(row.`Case Number`)) })
  SET n.`Case Number` = toFloat(trim(row.`Case Number`))
  SET n.`Raw Text` = row.`Raw Text`
  SET n.`Link` = row.`Link`
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ( $file_0) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Civil_Code_ID` { `Civil Code ID`: toInteger(trim(row.`Civil Code ID`)) })
  MATCH (target: `Inheritance_Code` { `Inheritance Code`: row.`Inheritance Code` })
  MERGE (source)-[r: `HAS_DETAIL`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ( $file_1) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Case_Number` { `Case Number`: toFloat(trim(row.`Case Number`)) })
  MATCH (target: `Court_Finding` { `Court Finding`: row.`Court Finding` })
  MERGE (source)-[r: `HAS_COURT_FINDING`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ( $file_1) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Case_Number` { `Case Number`: toFloat(trim(row.`Case Number`)) })
  MATCH (target: `Civil_Code_ID` { `Civil Code ID`: toInteger(trim(row.`Civil Code ID`)) })
  MERGE (source)-[r: `HAS_CIVIL_CODE`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ( $file_1) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Case_Number` { `Case Number`: toFloat(trim(row.`Case Number`)) })
  MATCH (target: `Court_Opinion` { `Court Opinion`: row.`Court Opinion` })
  MERGE (source)-[r: `HAS_COURT_OPINION`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ( $file_1) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Case_Number` { `Case Number`: toFloat(trim(row.`Case Number`)) })
  MATCH (target: `Court_Judgment` { `Court Judgment`: row.`Court Judgment` })
  MERGE (source)-[r: `HAS_COURT_JUDGEMENT`]->(target)
} IN TRANSACTIONS OF 10000 ROWS;
