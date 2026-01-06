# AWS MWAA Debug Template for Agentic AI

This simplified template helps customers debug AWS Managed Workflows for Apache Airflow (MWAA) issues using agentic AI. Just fill out basic info and upload your files - the AI will figure out the rest.

## Quick Start

1. **Fill out `AGENTS.md`** with minimal environment details
2. **Drop your files** into the appropriate folders
3. **Upload to agentic AI** for comprehensive analysis

## Folder Structure

```
mwaa-debug-template/
├── README.md                 # This file
├── AGENTS.md                 # Basic environment info (fill this out)
├── config/                   # Custom airflow.cfg overrides (if any)
├── dags/                     # Your DAG files and modules
├── logs/                     # Raw log files from CloudWatch
└── support/                  # Optional context files
```

## What to Include

### Required
- `AGENTS.md` - Fill out the basic info
- DAG files causing issues
- Recent log files from CloudWatch

### Optional
- Custom airflow.cfg overrides
- Supporting Python modules
- Timeline of issues (if helpful)

## Usage

Upload this package to your AI assistant with:

```
I'm experiencing AWS MWAA issues. Please analyze my environment using the AGENTS.md context and examine all files to identify problems and provide solutions. Focus on performance issues, DAG parsing problems, and best practices violations.
```