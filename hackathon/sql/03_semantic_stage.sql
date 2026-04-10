-- Internal stage for Cortex Analyst semantic model YAML (upload via PUT or Snowsight).
CREATE STAGE IF NOT EXISTS HACKATHON.DATA.SEMANTIC_MODELS
  DIRECTORY = ( ENABLE = TRUE )
  COMMENT = 'Cortex Analyst semantic YAML for Economic Intelligence';

-- After uploading economic_model.yaml:
-- LIST @HACKATHON.DATA.SEMANTIC_MODELS;
-- Cortex Analyst REST: "semantic_model_file": "@HACKATHON.DATA.SEMANTIC_MODELS/economic_model.yaml"
