from typing import Optional
from pyspark.sql import DataFrame
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_patient_master_profile(claims_table: str = "brnz_claims",
                                 problems_table: str = "silver_problems",
                                 medications_table: str = "silver_medications") -> Optional[DataFrame]:
    """
    Generate analytical patient master profile by joining claims, problems, and medications.
    
    Creates a comprehensive patient profile combining:
    - Claims: Total claims count and financial metrics
    - Problems: Active clinical problems list
    - Medications: Active medications list
    
    Uses FULL OUTER JOIN to retain all patient records even if they only have
    claims or only clinical data.
    
    Args:
        claims_table: Name of the bronze claims table
        problems_table: Name of the silver problems table
        medications_table: Name of the silver medications table
    
    Returns:
        DataFrame with patient master profile, or None if error occurs
    
    Raises:
        Exception: Logs error and returns None instead of raising
    """
    try:
        logger.info("Starting patient master profile generation")
        logger.info(f"Source tables - Claims: {claims_table}, Problems: {problems_table}, Medications: {medications_table}")
        
        consumption_query = f"""
        -- Generate the analytical patient master profile
        
        WITH aggregated_claims AS (
          SELECT 
            MemberID as patient_id,
            COUNT(DISTINCT ClaimID) AS total_claims_count,
            SUM(AllowedAmount) AS total_allowed_financials,
            SUM(PaidAmount) AS total_paid_financials
          FROM {claims_table}
          GROUP BY patient_id
        ),
        collected_problems AS (
          SELECT 
            patient_id,
            collect_list(named_struct(
              'problem_name', problem_name
            )) AS active_problems
          FROM {problems_table}
          GROUP BY patient_id
        ),
        collected_medications AS (
          SELECT 
            patient_id,
            collect_list(named_struct(
              'medication_name', medication_name
            )) AS active_medications
          FROM {medications_table}
          GROUP BY patient_id
        )
        -- Outer Join ensures we retain record history even if a patient only has claims or only clinical files
        SELECT 
          DISTINCT 
          COALESCE(c.patient_id, m.patient_id, p.patient_id) AS patient_id,
          c.total_claims_count,
          c.total_allowed_financials,
          c.total_paid_financials,
          p.active_problems,
          m.active_medications
        FROM aggregated_claims c
        FULL OUTER JOIN collected_medications m ON c.patient_id = m.patient_id
        FULL OUTER JOIN collected_problems p ON COALESCE(c.patient_id, m.patient_id) = p.patient_id
        """
        
        # Execute the SQL query
        profile_df = spark.sql(consumption_query)
        
        # Validate results
        row_count = profile_df.count()
        if row_count == 0:
            logger.warning("No patient profiles generated")
            return None
        
        logger.info(f"Successfully generated {row_count} patient profiles")
        
        return profile_df
        
    except Exception as e:
        logger.error(f"Error creating patient master profile: {str(e)}", exc_info=True)
        return None


def save_profile_to_gold(profile_df: DataFrame,
                        target_table: str = "gold_patient_master",
                        mode: str = "overwrite") -> bool:
    """
    Save patient master profile to gold layer table.
    
    Args:
        profile_df: DataFrame containing patient master profile
        target_table: Name of the target gold table
        mode: Write mode (overwrite, append, etc.)
    
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Saving patient master profile to {target_table} (mode: {mode})")
        
        profile_df.write.format("delta").mode(mode).saveAsTable(target_table)
        
        logger.info(f"Successfully saved to {target_table}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving profile to gold: {str(e)}", exc_info=True)
        return False


def consumption_pipeline(claims_table: str = "brnz_claims",
                        problems_table: str = "silver_problems",
                        medications_table: str = "silver_medications",
                        target_table: str = "gold_patient_master",
                        save_to_table: bool = True) -> Optional[DataFrame]:
    """
    Complete pipeline to create and save patient master profile to gold layer.
    
    Args:
        claims_table: Source bronze claims table
        problems_table: Source silver problems table
        medications_table: Source silver medications table
        target_table: Target gold table name
        save_to_table: Whether to save results to table
    
    Returns:
        DataFrame with patient master profile, or None if error occurs
    """
    logger.info("="*60)
    logger.info("Starting Gold Layer Consumption Pipeline")
    logger.info("="*60)
    
    # Create patient master profile
    profile_df = create_patient_master_profile(claims_table, problems_table, medications_table)
    
    if profile_df is None:
        logger.error("Pipeline failed: Could not create patient master profile")
        return None
    
    # Save to gold layer if requested
    if save_to_table:
        success = save_profile_to_gold(profile_df, target_table)
        if not success:
            logger.warning("Pipeline completed profile creation but failed to save")
        else:
            logger.info("✓ Pipeline completed successfully")
    else:
        logger.info("✓ Pipeline completed (save skipped)")
    
    return profile_df

if __name__ == "__main__":
    result_df = consumption_pipeline(
        claims_table="brnz_claims",
        problems_table="silver_problems",
        medications_table="silver_medications",
        target_table="gold_patient_master",
        save_to_table=True
    )
