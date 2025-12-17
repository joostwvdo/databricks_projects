#!/usr/bin/env python3
"""
Databricks MCP Server voor Football Data Project
"""
import os
import sys
import asyncio
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from databricks.sdk import WorkspaceClient
import json

# Initialiseer Databricks client
w = WorkspaceClient(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

# MCP Server
app = Server("databricks-football")

@app.list_tools()
async def list_tools() -> list[Tool]:
    """Lijst van beschikbare tools"""
    return [
        Tool(
            name="query_sql",
            description="Voer een SQL query uit op Unity Catalog",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "SQL query om uit te voeren"
                    },
                    "warehouse_id": {
                        "type": "string",
                        "description": "SQL Warehouse ID (optioneel)"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_matches",
            description="Haal wedstrijden op uit de database",
            inputSchema={
                "type": "object",
                "properties": {
                    "competition": {
                        "type": "string",
                        "description": "Competitie code (DED, PL, PD, SA)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Aantal wedstrijden",
                        "default": 10
                    }
                }
            }
        ),
        Tool(
            name="get_standings",
            description="Haal de huidige standen op",
            inputSchema={
                "type": "object",
                "properties": {
                    "competition": {
                        "type": "string",
                        "description": "Competitie code (DED, PL, PD, SA)"
                    }
                }
            }
        ),
        Tool(
            name="get_head_to_head",
            description="Haal head-to-head statistieken op",
            inputSchema={
                "type": "object",
                "properties": {
                    "team1": {
                        "type": "string",
                        "description": "Eerste team naam"
                    },
                    "team2": {
                        "type": "string",
                        "description": "Tweede team naam"
                    }
                },
                "required": ["team1", "team2"]
            }
        ),
        Tool(
            name="get_data_quality",
            description="Haal de laatste data quality test resultaten op",
            inputSchema={
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "description": "Filter op status (pass/fail)",
                        "enum": ["pass", "fail", "all"]
                    }
                }
            }
        ),
        Tool(
            name="run_pipeline",
            description="Trigger de Football Data pipeline job",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Voer een tool uit"""
    
    if name == "query_sql":
        query = arguments["query"]
        warehouse_id = arguments.get("warehouse_id")
        
        # Vind warehouse
        if not warehouse_id:
            warehouses = list(w.warehouses.list())
            if not warehouses:
                return [TextContent(type="text", text="Geen SQL Warehouse gevonden")]
            warehouse_id = warehouses[0].id
        
        # Voer query uit
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="30s"
        )
        
        if result.status.state == "SUCCEEDED":
            # Format resultaten
            rows = []
            if result.result and result.result.data_array:
                columns = [col.name for col in result.manifest.schema.columns]
                for row in result.result.data_array[:100]:  # Max 100 rijen
                    rows.append(dict(zip(columns, row)))
            
            return [TextContent(
                type="text", 
                text=json.dumps(rows, indent=2, default=str)
            )]
        else:
            return [TextContent(
                type="text",
                text=f"Query failed: {result.status.error}"
            )]
    
    elif name == "get_matches":
        competition = arguments.get("competition", "")
        limit = arguments.get("limit", 10)
        
        query = f"""
        SELECT 
            match_date,
            competition_name,
            home_team,
            away_team,
            home_score,
            away_score,
            status
        FROM football_data.gold.fct_match_results
        {'WHERE competition_code = ' + repr(competition) if competition else ''}
        ORDER BY match_date DESC
        LIMIT {limit}
        """
        
        return await call_tool("query_sql", {"query": query})
    
    elif name == "get_standings":
        competition = arguments.get("competition", "")
        
        query = f"""
        SELECT 
            team_name,
            position,
            played_games,
            won,
            draw,
            lost,
            points,
            goals_for,
            goals_against,
            goal_difference
        FROM football_data.gold.dim_teams
        {'WHERE competition_code = ' + repr(competition) if competition else ''}
        ORDER BY position
        """
        
        return await call_tool("query_sql", {"query": query})
    
    elif name == "get_head_to_head":
        team1 = arguments["team1"]
        team2 = arguments["team2"]
        
        query = f"""
        SELECT *
        FROM football_data.gold.agg_head_to_head
        WHERE (team1_name LIKE '%{team1}%' AND team2_name LIKE '%{team2}%')
           OR (team1_name LIKE '%{team2}%' AND team2_name LIKE '%{team1}%')
        """
        
        return await call_tool("query_sql", {"query": query})
    
    elif name == "get_data_quality":
        status_filter = arguments.get("status", "all")
        
        query = f"""
        SELECT 
            test_name,
            test_category,
            status,
            rows_affected,
            error_message,
            execution_time
        FROM football_data.gold.dq_test_results
        {'WHERE status = ' + repr(status_filter) if status_filter != 'all' else ''}
        ORDER BY execution_time DESC
        LIMIT 50
        """
        
        return await call_tool("query_sql", {"query": query})
    
    elif name == "run_pipeline":
        job_id = 750802098699777
        
        try:
            run = w.jobs.run_now(job_id=job_id)
            return [TextContent(
                type="text",
                text=f"Pipeline gestart!\nRun ID: {run.run_id}\nLink: {os.getenv('DATABRICKS_HOST')}/jobs/{job_id}/runs/{run.run_id}"
            )]
        except Exception as e:
            return [TextContent(
                type="text",
                text=f"Fout bij starten pipeline: {str(e)}"
            )]
    
    return [TextContent(type="text", text=f"Onbekende tool: {name}")]


async def main():
    """Start de MCP server"""
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main())
