#!/usr/bin/env python3
"""
PostgreSQL Partition Management Script

Automatically creates future partitions for the `logs` table and
drops partitions older than the specified retention period.
Designed to be run safely via a daily or weekly cron job.
"""

import argparse
import asyncio
import logging
import re
from datetime import datetime

import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("manage_partitions")

def add_months(sourcedate: datetime, months: int) -> datetime:
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    return datetime(year, month, 1)


async def manage_partitions(db_url: str, create_months: int, retain_months: int):
    logger.info(f"Connecting to database...")
    conn = await asyncpg.connect(db_url)
    
    try:
        now = datetime.now()
        
        # 1. Create future partitions
        logger.info(f"Ensuring partitions exist for the next {create_months} months...")
        for i in range(create_months + 1):
            target_date = add_months(now, i)
            start_of_month = target_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            end_of_month = add_months(start_of_month, 1)
            
            partition_name = f"logs_{start_of_month.strftime('%Y_%m')}"
            start_str = start_of_month.strftime('%Y-%m-%d')
            end_str = end_of_month.strftime('%Y-%m-%d')
            
            # Defensive validation against DDL injection (since asyncpg cannot parameterize DDL)
            if not re.match(r"^logs_\d{4}_\d{2}$", partition_name):
                raise ValueError(f"Invalid partition name format: {partition_name}")
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", start_str) or not re.match(r"^\d{4}-\d{2}-\d{2}$", end_str):
                raise ValueError("Invalid date string format")
            
            query = f"""
                CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF logs
                FOR VALUES FROM ('{start_str}') TO ('{end_str}');
            """
            await conn.execute(query)
            logger.info(f"Ensured partition exists: {partition_name} ({start_str} to {end_str})")
            
        # 2. Drop old partitions
        logger.info(f"Enforcing retention policy of {retain_months} months...")
        cutoff_date = add_months(now, -retain_months)
        cutoff_start_of_month = cutoff_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # Fetch all existing child tables of 'logs'
        # In PostgreSQL, partitions are inherited tables.
        partitions_query = """
            SELECT child.relname AS partition_name
            FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child ON pg_inherits.inhrelid = child.oid
            WHERE parent.relname = 'logs';
        """
        rows = await conn.fetch(partitions_query)
        
        for row in rows:
            p_name = row["partition_name"]
            # Expected format: logs_YYYY_MM
            try:
                parts = p_name.split('_')
                if len(parts) == 3 and parts[0] == "logs":
                    year = int(parts[1])
                    month = int(parts[2])
                    p_date = datetime(year, month, 1)
                    
                    if p_date < cutoff_start_of_month:
                        logger.warning(f"Dropping expired partition: {p_name}")
                        await conn.execute(f"DROP TABLE IF EXISTS {p_name};")
            except Exception as e:
                logger.error(f"Error evaluating partition {p_name}: {e}")
                
        logger.info("Partition management complete.")
        
    finally:
        await conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage PostgreSQL partitions for LogFoundry.")
    parser.add_argument("--db-url", default="postgresql://postgres:postgres@localhost:5432/logfoundry", help="PostgreSQL connection string")
    parser.add_argument("--create-months", type=int, default=6, help="Number of future months to create partitions for")
    parser.add_argument("--retain-months", type=int, default=6, help="Number of past months to retain")
    
    args = parser.parse_args()
    
    asyncio.run(manage_partitions(args.db_url, args.create_months, args.retain_months))
