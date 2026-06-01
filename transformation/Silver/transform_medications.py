from typing import Optional
from pyspark.sql import DataFrame
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_medications_from_ccd(source_table: str = "brnz_ccd") -> Optional[DataFrame]:
    """
    Extract and transform medication data from CCD bronze table.
    
    Extracts medication information from the nested CCD XML structure,
    flattening the substanceAdministration data and zipping arrays to create
    a normalized view of patient medications with their associated codes.
    
    Args:
        source_table: Name of the bronze CCD table to query
    
    Returns:
        DataFrame with columns: id, code_system, code_system_name, medication_name
        Returns None if error occurs
    
    Raises:
        Exception: Logs error and returns None instead of raising
    """
    try:
        logger.info(f"Starting medication extraction from table: {source_table}")
        
        medications_query = f"""
        SELECT 'patient_004' patient_id,
          inline(arrays_zip(id, code_system, code_system_name, medication_name)) AS (
            id, code_system, code_system_name, medication_name
          )
        FROM
          (
            SELECT DISTINCT
              body_component.entry.substanceAdministration.consumable.manufacturedProduct.manufacturedMaterial.code._code as id,
              body_component.entry.substanceAdministration.consumable.manufacturedProduct.manufacturedMaterial.code._codeSystem as code_system,
              body_component.entry.substanceAdministration.consumable.manufacturedProduct.manufacturedMaterial.code._codeSystemName as code_system_name,
              body_component.entry.substanceAdministration.consumable.manufacturedProduct.manufacturedMaterial.code._displayName as medication_name
            FROM
              {source_table}
              LATERAL VIEW EXPLODE(component.structuredBody.component.section) AS body_component
          )
        WHERE
          id[0] IS NOT NULL
        """
        
        # Execute the SQL query
        medications_df = spark.sql(medications_query)
        
        # Validate results
        row_count = medications_df.count()
        if row_count == 0:
            logger.warning(f"No medication data found in {source_table}")
            return None
        
        logger.info(f"Successfully extracted {row_count} medication records")
        
        return medications_df
        
    except Exception as e:
        logger.error(f"Error extracting medications from CCD: {str(e)}", exc_info=True)
        return None


def save_medications_to_silver(medications_df: DataFrame, 
                              target_table: str = "silver_medications",
                              mode: str = "overwrite") -> bool:
    """
    Save transformed medication data to silver layer table.
    
    Args:
        medications_df: DataFrame containing transformed medication data
        target_table: Name of the target silver table
        mode: Write mode (overwrite, append, etc.)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Saving medications data to {target_table} (mode: {mode})")
        
        medications_df.write.format("delta").mode(mode).saveAsTable(target_table)
        
        logger.info(f"Successfully saved to {target_table}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving medications to silver: {str(e)}", exc_info=True)
        return False


def transform_medications_pipeline(source_table: str = "brnz_ccd",
                                  target_table: str = "silver_medications",
                                  save_to_table: bool = True) -> Optional[DataFrame]:
    """
    Complete pipeline to extract, transform, and load medication data.
    
    Args:
        source_table: Source bronze table name
        target_table: Target silver table name
        save_to_table: Whether to save results to table
    
    Returns:
        DataFrame with transformed medications, or None if error occurs
    """
    logger.info("="*60)
    logger.info("Starting Medications Transformation Pipeline")
    logger.info("="*60)
    
    # Extract and transform
    medications_df = extract_medications_from_ccd(source_table)
    
    if medications_df is None:
        logger.error("Pipeline failed: Could not extract medications")
        return None
    
    # Save to silver layer if requested
    if save_to_table:
        success = save_medications_to_silver(medications_df, target_table)
        if not success:
            logger.warning("Pipeline completed extraction but failed to save")
        else:
            logger.info("✓ Pipeline completed successfully")
    else:
        logger.info("✓ Pipeline completed (save skipped)")
    
    return medications_df

if __name__ == "__main__":
    result_df = transform_medications_pipeline(
        source_table="brnz_ccd",
        target_table="silver_medications",
        save_to_table=True
    )
