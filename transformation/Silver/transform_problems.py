from typing import Optional
from pyspark.sql import DataFrame
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_problems_from_ccd(source_table: str = "brnz_ccd") -> Optional[DataFrame]:
    """
    Extract and transform problem data from CCD bronze table.
    
    Extracts problem observations from the nested CCD XML structure,
    flattening the data and zipping arrays to create a normalized view
    of patient problems with their associated codes.
    
    Args:
        source_table: Name of the bronze CCD table to query
    
    Returns:
        DataFrame with columns: id, code, code_system, code_system_name, problem_name
        Returns None if error occurs
    
    Raises:
        Exception: Logs error and returns None instead of raising
    """
    try:
        logger.info(f"Starting problem extraction from table: {source_table}")
        
        problems_query = f"""
        SELECT
          'patient_004' patient_id,
          inline(arrays_zip(id, code, code_system, code_system_name, problem_name)) AS (
            id, code, code_system, code_system_name, problem_name
          )
        FROM
          (
            SELECT DISTINCT
              body_component.entry.act.entryRelationship.observation.text.reference._value as id,
              body_component.entry.act.entryRelationship.observation.value._code as code,
              body_component.entry.act.entryRelationship.observation.value._codeSystem as code_system,
              body_component.entry.act.entryRelationship.observation.value._codeSystemName as code_system_name,
              body_component.entry.act.entryRelationship.observation.value._displayName as problem_name
            FROM
              {source_table}
              LATERAL VIEW EXPLODE(component.structuredBody.component.section) AS body_component
          )
        WHERE
          id[0] IS NOT NULL
        """
        
        # Execute the SQL query
        problems_df = spark.sql(problems_query)
        
        # Validate results
        row_count = problems_df.count()
        if row_count == 0:
            logger.warning(f"No problem data found in {source_table}")
            return None
        
        logger.info(f"Successfully extracted {row_count} problem records")
        
        return problems_df
        
    except Exception as e:
        logger.error(f"Error extracting problems from CCD: {str(e)}", exc_info=True)
        return None


def save_problems_to_silver(problems_df: DataFrame, 
                           target_table: str = "silver_problems",
                           mode: str = "overwrite") -> bool:
    """
    Save transformed problem data to silver layer table.
    
    Args:
        problems_df: DataFrame containing transformed problem data
        target_table: Name of the target silver table
        mode: Write mode (overwrite, append, etc.)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Saving problems data to {target_table} (mode: {mode})")
        
        problems_df.write.format("delta").mode(mode).saveAsTable(target_table)
        
        logger.info(f"Successfully saved to {target_table}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving problems to silver: {str(e)}", exc_info=True)
        return False


def transform_problems_pipeline(source_table: str = "brnz_ccd",
                               target_table: str = "silver_problems",
                               save_to_table: bool = True) -> Optional[DataFrame]:
    """
    Complete pipeline to extract, transform, and load problem data.
    
    Args:
        source_table: Source bronze table name
        target_table: Target silver table name
        save_to_table: Whether to save results to table
    
    Returns:
        DataFrame with transformed problems, or None if error occurs
    """
    logger.info("="*60)
    logger.info("Starting Problems Transformation Pipeline")
    logger.info("="*60)
    
    # Extract and transform
    problems_df = extract_problems_from_ccd(source_table)
    
    if problems_df is None:
        logger.error("Pipeline failed: Could not extract problems")
        return None
    
    # Save to silver layer if requested
    if save_to_table:
        success = save_problems_to_silver(problems_df, target_table)
        if not success:
            logger.warning("Pipeline completed extraction but failed to save")
        else:
            logger.info("✓ Pipeline completed successfully")
    else:
        logger.info("✓ Pipeline completed (save skipped)")
    
    return problems_df

if __name__ == "__main__":
    result_df = transform_problems_pipeline(
        source_table="brnz_ccd",
        target_table="silver_problems",
        save_to_table=True
    )
