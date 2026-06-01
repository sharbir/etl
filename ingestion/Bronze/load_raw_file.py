from typing import Optional
from pyspark.sql import DataFrame
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_ccd_data(file_path: str = "/Volumes/workspace/default/hs/sample_ccd_100_records.xml",
                  table_name: str = "brnz_ccd") -> Optional[DataFrame]:
    """
    Load XML CCD file from volume and create Delta table.
    
    Args:
        file_path: Path to the XML file in volume
        table_name: Name of the target Delta table
    
    Returns:
        DataFrame if successful, None if error occurs
    
    Raises:
        Exception: Logs error and returns None instead of raising
    """
    try:
        logger.info(f"Loading CCD data from: {file_path}")
        
        # Load XML file from volume into Spark DataFrame
        df = spark.read.format("xml") \
            .option("rowTag", "ClinicalDocument") \
            .load(file_path)
        
        if df.count() == 0:
            logger.warning(f"No data found in {file_path}")
            return None
        
        logger.info(f"Successfully loaded {df.count()} records")
        
        # Write DataFrame to Delta table
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        logger.info(f"Data written to table: {table_name}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading CCD data: {str(e)}", exc_info=True)
        return None

def load_claims_data(file_path: str = "/Volumes/workspace/default/hs/claims_data.csv",
                      table_name: str = "brnz_claims") -> Optional[DataFrame]:
    """
    Load CSV claims file from volume and create Delta table.
    
    Args:
        file_path: Path to the CSV file in volume
        table_name: Name of the target Delta table
    
    Returns:
        DataFrame if successful, None if error occurs
    
    Raises:
        Exception: Logs error and returns None instead of raising
    """
    try:
        logger.info(f"Loading claims data from: {file_path}")
        
        # Load CSV file from volume into Spark DataFrame
        df = spark.read.format("csv") \
            .option("header", True) \
            .option("inferSchema", True) \
            .load(file_path)
        
        if df.count() == 0:
            logger.warning(f"No data found in {file_path}")
            return None
        
        logger.info(f"Successfully loaded {df.count()} records")
        
        # Write DataFrame to Delta table
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        logger.info(f"Data written to table: {table_name}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading claims data: {str(e)}", exc_info=True)
        return None

def main() -> bool:
    """
    Main function to orchestrate data loading operations.
    
    Returns:
        True if all operations successful, False otherwise
    """
    success = True
    
    # Load CCD data
    logger.info("Starting CCD data load...")
    ccd_df = load_ccd_data()
    
    if ccd_df is not None:
        logger.info("✓ CCD data loaded successfully")
        print("✓ CCD data loaded successfully")
    else:
        logger.error("✗ Failed to load CCD data")
        print("✗ Failed to load CCD data")
        success = False
    
    # Load Claims data
    logger.info("Starting claims data load...")
    claims_df = load_claims_data()
    
    if claims_df is not None:
        logger.info("✓ Claims data loaded successfully")
        print("✓ Claims data loaded successfully")
    else:
        logger.error("✗ Failed to load claims data")
        print("✗ Failed to load claims data")
        success = False
    
    # Summary
    if success:
        logger.info("All data loading operations completed successfully")
        print("\n" + "="*50)
        print("All data loading operations completed successfully")
        print("="*50)
    else:
        logger.warning("Some data loading operations failed")
        print("\n" + "="*50)
        print("⚠ Some data loading operations failed - check logs")
        print("="*50)
    
    return success

if __name__ == "__main__":
    main()
